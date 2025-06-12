import asyncio
import uuid
import math
import datetime as _dt
from typing import List, Iterable, Dict, Tuple, Optional

import pandas as pd
from sqlalchemy import (
    Integer, Column, String, JSON, func, DateTime, ForeignKey, text, select,
    BIGINT, Boolean, Float, and_, delete, or_, update,
    CheckConstraint, ARRAY, distinct, tuple_, UniqueConstraint
)

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import relationship, selectinload, Mapped, mapped_column
from sqlalchemy.sql.functions import count, concat
import logging
from client_server.schemas.trade import SettingsModel, StageConfigModel
from database import engine, Base, get_session, connection, SessionLocal
from datetime import datetime, timedelta
import sqlalchemy_enums as s_enums
from sqlalchemy_enums import MarketEnum, ReasonEnum


reasons = s_enums.ReasonEnum  # YELLOW - RSI, RED - CCI

logger = logging.getLogger(__name__)

def auth_api(func):
    """
    Декоратор проверяет, что API-ключ принадлежит пользователю.
    Работает строго в одном greenlet-контексте: создаём ОДНУ AsyncSession
    и передаём её дальше.
    """
    async def wrapper(*args, **kwargs):
        api_id  = kwargs.get("api_id", "")
        user_id = kwargs.get("user_id", "")

        async with SessionLocal() as session:               # ← 1) одна сессия
            api = await session.scalar(                     # ← 2) запрос через неё
                select(Api_Key).where(Api_Key.pub_id == api_id)
            )
            if api is None or api.user_id != user_id:
                return None

            # 3) передаём ТУ ЖЕ сессию дальше
            return await func(*args, session=session, **kwargs)

    return wrapper



def get_stage_from_stuck(stuck: int) -> int:
    """
    Пример конвертации количества «зависших» сделок в этап:
      - 0 «зависших» => 1-й этап
      - 1 «зависшая» => 2-й этап
      - 2 или больше => 3-й этап
    При желании усложните.
    """
    return min(stuck + 1, 3)

@connection
async def get_all_stage_limits(
    api_id: str,
    *,
    session: AsyncSession
) -> dict[int, dict[ReasonEnum, int]]:
    """
    Возвращает структуру:
      { stage: { ReasonEnum.YELLOW: rsi_slots,
                 ReasonEnum.RED   : cci_slots } }
    """
    res = await session.execute(
        select(StageConfig).where(StageConfig.api_id == api_id)
    )
    out: dict[int, dict[ReasonEnum, int]] = {}
    for cfg in res.scalars():
        out[cfg.stage] = {
            ReasonEnum.YELLOW: cfg.rsi_slots,
            ReasonEnum.RED: cfg.cci_slots,
        }
    return out


def get_cumulative_limit(stage: int, reason: reasons) -> int:
    """
    Суммируем лимиты от 1 до stage включительно для данного 'reason'.
    Если stage=2 => берём STAGE_CONFIG[1][reason] + STAGE_CONFIG[2][reason].
    Если reason нет в конфиге, считаем 0.
    """
    max_stage = len(STAGE_CONFIG)  # например, 3
    df = pd.DataFrame(STAGE_CONFIG)
    if stage < 1:
        stage = 1
    if stage > max_stage:
        stage = max_stage
    df_cumsum = df.cumsum()
    total = df_cumsum.iloc[stage - 1][reason]

    return total


class User(Base):
    login: Mapped[str] = mapped_column(String(64), nullable=False)
    password: Mapped[str] = mapped_column(String(256), nullable=False)
    public_id: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    email: Mapped[str] = mapped_column(String(256), nullable=False)
    api_keys: Mapped[list["Api_Key"]] = relationship(cascade="all, delete-orphan")


class Api_Key(Base):
    api_key: Mapped[str] = mapped_column(String(256), nullable=False)
    secret_key: Mapped[str] = mapped_column(String(256), nullable=True)
    market: Mapped[s_enums.MarketEnum]
    user_id: Mapped[str] = mapped_column(String(256), ForeignKey('users.public_id'), nullable=False)
    pub_id: Mapped[str] = mapped_column(String(256), nullable=False, unique=True)
    running: Mapped[bool] = mapped_column(Boolean, nullable=False)
    orders: Mapped[list["Order"]] = relationship(cascade="all, delete-orphan")
    settings: Mapped[list["SymbolSettings"]] = relationship(cascade="all, delete-orphan")
    stage_config: Mapped[list["StageConfig"]] = relationship(cascade="all, delete-orphan")
    balance: Mapped[float] = mapped_column(Float, nullable=False)


# user: Mapped["Users"] = relationship("Users", back_populates="api_keys")

class Order(Base):
    order_id: Mapped[str] = mapped_column(String(256), nullable=False)
    order_type: Mapped[s_enums.OrderTypeEnum]
    position_side: Mapped[s_enums.PositionSideEnum]
    side: Mapped[s_enums.SideEnum]
    note: Mapped[s_enums.NoteEnum]

    price: Mapped[float] = mapped_column(Float, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    position: Mapped[str] = mapped_column(String(256), nullable=False)
    api_id: Mapped[str] = mapped_column(String(256), ForeignKey('api_keys.pub_id'), nullable=False)
    system_id: Mapped[str] = mapped_column(String(256), nullable=False)
    reason: Mapped[s_enums.ReasonEnum] = mapped_column(String(30), nullable=False, default=s_enums.ReasonEnum.RED)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    stage: Mapped[int] = mapped_column(Integer, default=0)
    # user: Mapped["Users"] = relationship("Users", back_populates="orders")


class SymbolSettings(Base):
    """
    Таблица для индивидуальных настроек по символам для конкретного API-ключа (api_id).
    """
    # id, created_at, updated_at
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    api_id: Mapped[str] = mapped_column(String(256), ForeignKey('api_keys.pub_id'), nullable=False)

    timeframe: Mapped[str] = mapped_column(String(32), default="15m")
    rsi_length: Mapped[int] = mapped_column(Integer, default=14)
    cci_length: Mapped[int] = mapped_column(Integer, default=20)

    rsi_threshold: Mapped[float] = mapped_column(Float, default=30.0)
    cci_threshold: Mapped[float] = mapped_column(Float, default=-100.0)

    use_delay_filter: Mapped[bool] = mapped_column(Boolean, default=True)  # использовать delay (смещение)

    delay_bars: Mapped[int] = mapped_column(Integer, default=2)  # сколько свечей надо ждать
    use_delta_filter: Mapped[bool] = mapped_column(Boolean, default=True)  # использовать delta-фильтр
    delta_filter: Mapped[float] = mapped_column(Float, default=0.0)  # если delta < delta_filter => skip

    # Флаги для включения/отключения проверки сигналов
    use_rsi: Mapped[bool] = mapped_column(Boolean, default=True)  # включена проверка RSI
    use_cci: Mapped[bool] = mapped_column(Boolean, default=True)  # включена проверка CCI


    use_short: Mapped[bool] = mapped_column(Boolean, default=True)
    sma_fast: Mapped[int] = mapped_column(Integer, default=20)
    sma_slow: Mapped[int] = mapped_column(Integer, default = 300)
    trailing_stop_percentage: Mapped[float] = mapped_column(Float, default=0.6)
    activation_price: Mapped[float] = mapped_column(Float, default=0.6)
    base_stop: Mapped[float] = mapped_column(Float, default=0.6)
    leverage: Mapped[int] = mapped_column(Integer, default=1)

    take_profit_percent: Mapped[float] = mapped_column(Float, default=1.0)
    initial_entry_percent: Mapped[float] = mapped_column(Float, default=5.0)
    averaging_levels: Mapped[int] = mapped_column(Integer, default=3)
    averaging_percents: Mapped[str] = mapped_column(String(256), default="2,4,6")
    averaging_volume_percents: Mapped[str] = mapped_column(String(256), default="10,20,30")
    use_balance_percent: Mapped[float] = mapped_column(Float, default=100.0)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    stage: Mapped[int] = mapped_column(Integer, default=1)

    # хранить любые экзотические настройки
    settings_json: Mapped[dict] = mapped_column(JSONB, default={})
    __table_args__ = (
        CheckConstraint(take_profit_percent >= 0, name="take_profit_percent_positive", ),
        CheckConstraint(initial_entry_percent >= 0, name="initial_entry_percent_positive", ),
        CheckConstraint(averaging_levels >= 0, name="averaging_levels_positive", ),
        CheckConstraint(use_balance_percent >= 0, name="use_balance_percent_positive", ),
        {}
    )


class StageConfig(Base):
    __tablename__ = "stageconfigs"
    stage: Mapped[int] = mapped_column(Integer, nullable=False)
    rsi_slots: Mapped[int] = mapped_column(Integer, default=0)
    cci_slots: Mapped[int] = mapped_column(Integer, default=0)
    api_id: Mapped[str] = mapped_column(String(256), ForeignKey("api_keys.pub_id"))
    __table_args__ = (UniqueConstraint('stage', 'api_id', name='uq_stage_reason'),)


class Slot(Base):
    __tablename__ = "slots"
    api_id: Mapped[str] = mapped_column(String(256), ForeignKey('api_keys.pub_id'))
    position: Mapped[str] = mapped_column(String(128))
    reason: Mapped[s_enums.ReasonEnum]
    stage: Mapped[int] = mapped_column(Integer, default=1)
    averaging_count: Mapped[int] = mapped_column(Integer, default=0)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[_dt.datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[_dt.datetime] = mapped_column(DateTime, server_default=func.now(),
                                                     onupdate=func.now())


# ────────────────────────────────────────────────────────────────────────────────
#                   СТАРЫЕ МОДЕЛИ (User / Api_Key / Order / …)
#                   ─ полностью БЕЗ изменений ─
# ────────────────────────────────────────────────────────────────────────────────
# … весь прежний код классов User, Api_Key, Order, SymbolSettings остается
# … без изменений
# ────────────────────────────────────────────────────────────────────────────────
#             ↓↓↓ ДОБАВЛЕНЫ вспомогательные функции для новых таблиц ↓↓↓
# ────────────────────────────────────────────────────────────────────────────────
@connection
async def get_stage_limit(api_id: str, stage: int, reason: ReasonEnum, *, session: AsyncSession) -> int:
    """Сколько разрешённых слотов на данном этапе + reason."""
    cfg = (await session.execute(
        select(StageConfig).where(StageConfig.stage == stage, StageConfig.api_id == api_id)
    )).scalar()

    if cfg is None:
        return 0
    return cfg.rsi_slots if reason == ReasonEnum.YELLOW else cfg.cci_slots


def _time_ago(minutes: int) -> _dt.datetime:
    return _dt.datetime.now() - _dt.timedelta(minutes=minutes)

@connection
async def _stage_has_reason(api_id: str, stage: int, reason: ReasonEnum,
                            *, session: AsyncSession) -> bool:
    """True, если на этапе есть хотя бы один слот указанного reason."""
    return (await get_stage_limit(api_id, stage, reason)) > 0


@connection
async def add_slot(api_id: str, position: str, reason: ReasonEnum, stage: int,
                   *, session: AsyncSession) -> Slot:
    slot = Slot(api_id=api_id, position=position, reason=reason, stage=stage)
    session.add(slot)
    logger.info(f"Placing slot {position} {reason} {stage}")
    await session.commit()
    return slot


@connection
async def get_active_slot(api_id: str, position: str, *, session: AsyncSession) -> Slot | None:
    stmt = select(Slot).where(Slot.api_id == api_id, Slot.position == position, Slot.active == True)
    return (await session.execute(stmt)).scalar()

@connection
async def get_all_active_slots(session: AsyncSession) -> List[Slot]:
    stmt = select(Slot).where(Slot.active == True)
    return (await session.execute(stmt)).scalars().all()


@connection
async def count_active_slots(api_id: str, *, session: AsyncSession) -> int:
    """
    Сколько активных слотов (открытых монет) сейчас у данного API-ключа.
    Используется TradingBot для проверки лимита max_symbols.
    """
    stmt = (
        select(func.count(Slot.id))
        .where(Slot.api_id == api_id, Slot.active.is_(True))
    )
    return (await session.execute(stmt)).scalar() or 0


@connection
async def increment_avg_count(api_id: str, position: str, *, session: AsyncSession):
    stmt = (update(Slot)
            .where(Slot.api_id == api_id, Slot.position == position, Slot.active == True)
            .values(averaging_count=Slot.averaging_count + 1,
                    updated_at=func.now()))
    await session.execute(stmt)
    await session.commit()

@connection
async def close_slot(api_id: str, position: str, *, session: AsyncSession) -> Slot | None:
    stmt = (
        update(Slot)
        .where(
            Slot.api_id == api_id,
            Slot.position == position,
            Slot.active.is_(True),
        )
        .values(active=False, updated_at=func.now())
        .returning(Slot)
    )
    closed: Slot | None = (await session.execute(stmt)).scalar()
    await session.commit()

    # ───── ТРИГГЕРИМ «Тетрис» НЕМЕДЛЕННО ─────
    if closed:
        # отдельная сессия внутри tetris_move() будет создана автоматически
        await tetris_move(api_id, closed.stage, closed.reason)

    return closed

@connection
async def is_reason_stuck(
    api_id: str,
    stage: int,
    reason: ReasonEnum,
    lim: int,
    *,
    session: AsyncSession,
) -> bool:
    """
    True, если конкретный signal-type (reason) «залип» на этапе:
      • заняты все лимит-слоты lim
      • каждая позиция достигла max averaging_count
      • прошло ≥5 минут с момента последнего усреднения
    """
    if lim == 0:
        return False

    stmt = (
        select(func.count())
        .select_from(Slot)
        .join(
            SymbolSettings,
            and_(
                SymbolSettings.symbol == Slot.position,
                SymbolSettings.api_id == Slot.api_id,
            ),
        )
        .where(
            Slot.api_id == api_id,
            Slot.stage == stage,
            Slot.reason == reason,
            Slot.active.is_(True),
            Slot.averaging_count >= SymbolSettings.averaging_levels,
            Slot.updated_at <= datetime.utcnow() - timedelta(minutes=5),
        )
    )
    cnt = await session.scalar(stmt) or 0
    return cnt >= lim

@connection
async def can_open_extra(
    api_id: str,
    reason: ReasonEnum,
    *,
    session: AsyncSession,
) -> tuple[bool, int]:
    """
    Возвращает (можно_открыть, stage_для_открытия).

    Логика:
      1. Все предыдущие этапы для нужного reason
         ── либо «залипли»,
         ── либо лимит по reason = 0 и тогда достаточно полного «залипания» этапа.
      2. На текущем этапе есть свободный слот.
    """
    limits_by_stage = await get_all_stage_limits(api_id)
    max_stage       = max(limits_by_stage.keys(), default=1)

    for stage in range(1, max_stage + 1):

        # ── 1. проверяем все предыдущие этапы ────────────────────────────────
        for prev in range(1, stage):
            lim_prev = limits_by_stage.get(prev, {}).get(reason, 0)

            # 1-а. Для reason, у которого на prev-этапе есть хотя бы 1 слот
            if lim_prev > 0:
                if not await is_reason_stuck(api_id, prev, reason, lim_prev):
                    return False, stage
            # 1-б. Для reason со слотом = 0 считаем его «залипшим» автоматически,
            #      но требуем, чтобы ОСТАЛЬНЫЕ типы сигналов на этапе залипли полностью.
            else:
                y_lim = limits_by_stage.get(prev, {}).get(ReasonEnum.YELLOW, 0)
                r_lim = limits_by_stage.get(prev, {}).get(ReasonEnum.RED,    0)

                y_ok = True if y_lim == 0 else await is_reason_stuck(
                    api_id, prev, ReasonEnum.YELLOW, y_lim
                )
                r_ok = True if r_lim == 0 else await is_reason_stuck(
                    api_id, prev, ReasonEnum.RED,   r_lim
                )

                if not (y_ok and r_ok):
                    return False, stage

        # ── 2. На текущем этапе есть свободный слот ──────────────────────────
        cur_lim = limits_by_stage.get(stage, {}).get(reason, 0)
        if cur_lim == 0:
            continue   # у этого reason нет слотов на данном этапе

        busy = await session.scalar(
            select(func.count(Slot.id)).where(
                Slot.api_id == api_id,
                Slot.stage  == stage,
                Slot.reason == reason,
                Slot.active.is_(True),
            )
        ) or 0

        if busy < cur_lim:
            return True, stage

    return False, max_stage
@connection
async def is_stage_stuck(api_id: str, stage: int, reason: ReasonEnum,
                         *, session: AsyncSession) -> bool:
    """
    Этап считается залипшим, если:
      • заполнены все слоты *данного* reason
      • averaging_count >= max и прошло ≥ delay
      • одновременно на этом же этапе залип и противоположный reason,
        либо лимит противоположного reason = 0
    """
    lim_this = await get_stage_limit(api_id, stage, reason)
    if lim_this == 0:
        return False

    # проверяем текущий reason
    stuck_this = await _reason_stuck(api_id, stage, reason, lim_this)

    # проверяем «другой» reason
    other = ReasonEnum.RED if reason == ReasonEnum.YELLOW else ReasonEnum.YELLOW
    lim_other = await get_stage_limit(api_id, stage, other)
    stuck_other = True if lim_other == 0 else await _reason_stuck(
        api_id, stage, other, lim_other
    )

    return stuck_this and stuck_other

@connection
async def _reason_stuck(api_id: str, stage: int, reason: ReasonEnum,
                        lim: int, *, session: AsyncSession) -> bool:
    """
    Проверяет «залип» ли конкретный Reason на этапе.

    ИЗМЕНЕНИЕ:
      JOIN учитывает api_id берётся ровно та запись
      SymbolSettings, которая принадлежит ТЕКУЩЕМУ API‑ключу.
      Slot.averaging_count >= SymbolSettings.averaging_levels
      использует реальный лимит (у. = 1).
    """
    cnt_stmt = (
        select(func.count())
        .select_from(Slot)
        .join(
            SymbolSettings,
            and_(
                SymbolSettings.symbol == Slot.position,   # ← символ тот же
                SymbolSettings.api_id == Slot.api_id      # ← и тот же API-ключ
            )
        )
        .where(
            Slot.api_id == api_id,
            Slot.stage  == stage,
            Slot.reason == reason,
            Slot.active.is_(True),
            Slot.averaging_count >= SymbolSettings.averaging_levels,
            Slot.updated_at      <= _time_ago(5),
        )
    )
    cnt = await session.scalar(cnt_stmt) or 0
    return cnt >= lim


@connection
async def tetris_move(
    api_id: str,
    freed_stage: int,
    freed_reason: ReasonEnum,
    *,
    session: AsyncSession,
) -> None:
    """
    Перемещает последнюю монету назад («Тетрис»):

      • переносит stage
      • **сохраняет** averaging_count и время последнего усреднения
      • синхронизирует stage во всех активных ордерах
      • рекурсивно тянет цепочку дальше
    """
    cand_q = (
        select(Slot)
        .where(
            Slot.api_id == api_id,
            Slot.stage  > freed_stage,
            Slot.reason == freed_reason,
            Slot.active.is_(True),
        )
        .order_by(Slot.created_at.desc())      # самая свежая
        .limit(1)
        .with_for_update()
    )
    cand = await session.scalar(cand_q)
    if not cand:
        return

    old_stage = cand.stage
    cand.stage = freed_stage                   # ← ДВИГАЕМ этап…
    # cand.averaging_count = 0                # ✗ БОЛЬШЕ НЕ СБРАСЫВАЕМ
    # updated_at меняется автоматически через onupdate=func.now()

    await session.commit()

    # синхронизируем stage у ордеров
    await session.execute(
        update(Order)
        .where(
            Order.api_id  == api_id,
            Order.position == cand.position,
            Order.active.is_(True),
        )
        .values(stage = freed_stage)
    )
    await session.commit()

    # рекурсивно двигаем следующую монету
    await tetris_move(api_id, old_stage, freed_reason)


@connection
async def add_user(login: str, email: str, password: str, session: AsyncSession) -> User:
    uid = str(uuid.uuid4())
    new_user = User(login=login, password=password, public_id=uid, email=email)
    session.add(new_user)
    await session.commit()
    await session.refresh(new_user)
    return new_user


@connection
async def get_user(session: AsyncSession, public_id: str = None, email: str = None, login: str = None) -> User | None:
    conditions = [

    ]
    if public_id:
        conditions.append(User.public_id == public_id)
    if email:
        conditions.append(or_(User.email == email, User.login == email))
    if login:
        conditions.append(or_(User.login == login, User.email == login))

    stmt = select(User).where(*conditions)
    try:
        res = await session.execute(stmt)
        return res.scalar()
    except Exception as ex:
        print(ex)
        raise


@connection
async def add_api_key(api_key: str, secret_key: str, market: MarketEnum, user_id: str,
                      session: AsyncSession) -> Api_Key:
    uid = str(uuid.uuid4())
    new_api = Api_Key(api_key=api_key, secret_key=secret_key, market=market, user_id=user_id, pub_id=uid, balance=0,
                      running=True)
    session.add(new_api)
    await session.commit()
    await session.refresh(new_api)
    return new_api


@connection
async def get_api_by_key(api_key: str, secret_key: str, session: AsyncSession) -> Api_Key:
    stmt = select(Api_Key).where(Api_Key.api_key == api_key, Api_Key.secret_key == secret_key).options(
        selectinload(Api_Key.settings)
    )
    res = await session.execute(stmt)
    return res.scalar()


@connection
async def get_api_by_id(api_id: str, session: AsyncSession) -> Api_Key:
    stmt = select(Api_Key).where(Api_Key.pub_id == api_id).options(
        selectinload(Api_Key.settings), selectinload(Api_Key.stage_config)
    )
    res = await session.execute(stmt)
    return res.scalar()


@connection
async def add_order(order_id: str, order_type: s_enums.OrderTypeEnum, position_side: s_enums.PositionSideEnum,
                    side: s_enums.SideEnum, note: s_enums.NoteEnum, price: float,
                    quantity: float, position: str, api_id: str, system_id: str, reason: s_enums.ReasonEnum, stage: int,
                    session: AsyncSession) -> Order:
    new_order = Order(order_id=str(order_id), order_type=
    order_type,
                      position_side=position_side, side=side, note=note, price=price, quantity=quantity,
                      position=position, api_id=api_id, system_id=system_id, reason=reason, stage=stage)
    session.add(new_order)
    await session.commit()
    return new_order


@connection
async def get_user_api_keys(pub_id, session: AsyncSession) -> List[Api_Key]:
    stmt = select(Api_Key).where(Api_Key.user_id == pub_id)
    result = await session.execute(stmt)
    return result.scalars()


@connection
async def delete_api(api_id: str, user_id: str, session: AsyncSession) -> None:
    stmt = delete(Api_Key).where(Api_Key.pub_id == api_id, Api_Key.user_id == user_id)
    await session.execute(stmt)
    await session.commit()


@connection
async def update_api(api_id: str, user_id: str, payload: dict, session: AsyncSession) -> Api_Key:
    payload = {k: v for k, v in payload.items() if v is not None}
    stmt = update(Api_Key).where(Api_Key.pub_id == api_id, Api_Key.user_id == user_id).values(**payload)
    await session.execute(stmt)
    stmt = select(Api_Key).where(Api_Key.pub_id == api_id, Api_Key.user_id == user_id)
    ret = await session.execute(stmt)
    x = ret.first()[0]
    return x


@connection
async def get_user_orders(pub_id, session: AsyncSession) -> List[Order]:
    api_keys = await get_user_api_keys(pub_id, session)
    api = list(map(lambda x: x.pub_id, api_keys))
    stmt = select(Order).filter(
        Order.api_id.in_(api)
    )
    result = await session.execute(stmt)
    return result.scalars()


@connection
async def get_orders_by_key(api_key: str, secret_key: str, session: AsyncSession) -> List[Order]:
    stmt = select(Api_Key.orders).where(Api_Key.api_key == api_key)
    res = await session.execute(stmt)
    return res.scalars()


@connection
async def get_orders_by_conditions(conditions: Iterable, session: AsyncSession) -> List[Order]:
    stmt = select(Order).filter(and_(*conditions))
    result = await session.execute(stmt)
    return result.scalars().all()


@connection
async def get_apis(market: MarketEnum = None, session: AsyncSession = None) -> List[Api_Key]:
    if market is None:
        stmt = select(Api_Key).filter(Api_Key.running == True)
    else:
        stmt = select(Api_Key).filter(Api_Key.market == market, Api_Key.running == True)
    result = await session.execute(stmt)
    return result.scalars().all()


@connection
async def get_averaging_orders(api_id: str, symbol, session: AsyncSession) -> List[Order]:
    stmt = select(Order).filter(Order.api_id == api_id, Order.position == symbol,
                                Order.note == s_enums.NoteEnum.AVERAGING, Order.active == True)
    result = await session.execute(stmt)
    return result.scalars().all()


@connection
async def get_tp_order(api_id: str, symbol: str, session: AsyncSession) -> Order:
    stmt = select(Order).filter(Order.api_id == api_id,
                                Order.position == symbol,
                                Order.note == s_enums.NoteEnum.TAKE_PROFIT,
                                Order.active == True) \
        .order_by(Order.id.desc())
    result = await session.execute(stmt)
    return result.scalars().first()


@connection
async def delete_order(api_id: str, order_id: str, session: AsyncSession):
    stmt = delete(Order).filter(Order.api_id == api_id, Order.order_id == order_id)
    await session.execute(stmt)
    await session.commit()


@connection
async def set_balance(api_id: str, balance: float, session: AsyncSession):
    stmt = update(Api_Key).where(Api_Key.pub_id == api_id).values(balance=balance)
    await session.execute(stmt)
    await session.commit()


@connection
async def get_balance(api_id: str, session: AsyncSession) -> float:
    stmt = select(Api_Key.balance).where(Api_Key.pub_id == api_id)
    res = await session.execute(stmt)
    return res.scalar()


@connection
async def update_tp(system_id: str, api_id: str, price: float, qty: float, new_order_id: str,
                    session: AsyncSession) -> None:
    stmt = update(Order).where(Order.system_id == system_id, Order.api_id == api_id,
                               Order.note == s_enums.NoteEnum.TAKE_PROFIT) \
        .values(price=price, quantity=qty, order_id=str(new_order_id))
    await session.execute(stmt)
    await session.commit()


@connection
async def toggle_order(api_id: str, order_id: str, session: AsyncSession) -> None:
    stmt = update(Order).where(Order.order_id == order_id, Order.api_id == api_id).values(active=False)
    await session.execute(stmt)
    await session.commit()


@connection
async def get_timeframes_lens(session: AsyncSession) -> List[Tuple[str, tuple[int, ...], tuple[int, ...]]]:
    stmt = select(distinct(SymbolSettings.timeframe))
    res = await session.execute(stmt)
    tf = res.scalars().all()
    ret = []
    for t in tf:
        stmt = select(distinct(SymbolSettings.rsi_length)).filter(SymbolSettings.timeframe == t)
        res = await session.execute(stmt)
        l = res.scalars().all()
        stmt2 = select(distinct(SymbolSettings.cci_length)).filter(SymbolSettings.timeframe == t)
        res = await session.execute(stmt2)
        l2 = res.scalars().all()
        ret.append((t, l, l2))
    return ret

#NEW 05.05"""

@connection
async def get_timeframes_smas(session: AsyncSession):
    stmt = select(distinct(SymbolSettings.timeframe)).where(SymbolSettings.use_short == True)
    res = await session.execute(stmt)
    tf = res.scalars().all()
    ret = []
    for t in tf:
        stmt = select(SymbolSettings.sma_fast, SymbolSettings.sma_slow).filter(SymbolSettings.timeframe == t, SymbolSettings.use_short == True)
        res = await session.execute(stmt)
        l = res.all()
        #print(l)
        l = set(l)
        ret.append((t, l))
    return ret

@connection
async def get_shorts(symbol: str, timeframe: str, fast: int, slow: int, session: AsyncSession):
    out: List[Tuple[str, ReasonEnum]] = []


    # ───── 2. выбираем настройки С УЧЁТОМ timeframe и длин ─────
    stmt = select(SymbolSettings.api_id).where(
        SymbolSettings.symbol == symbol,
        SymbolSettings.active.is_(True),
        SymbolSettings.timeframe == timeframe,
        SymbolSettings.sma_fast == fast,
        SymbolSettings.sma_slow == slow,
        SymbolSettings.use_short == True
    )
    res = await session.execute(stmt)
    return res.scalars().all()


@connection
async def recursive_stage_update(
    api_id: str,
    stage: int,
    reason: ReasonEnum,
    *,
    session: AsyncSession,
):
    """
    Если на этапе stage+1 осталась монета нужного типа
    двигаем её назад, коммитим и зовём себя же для stage+1.
    Так продолжаем, пока не кончатся кандидаты.
    """
    # сколько этапов реально настроено для данного api_id
    max_stage = (
        await session.scalar(
            select(func.max(StageConfig.stage)).where(StageConfig.api_id == api_id)
        )
    ) or 0

    if stage > max_stage:
        return

    candidate_q = (
        select(Order)
        .where(
            Order.reason == reason,
            Order.stage > stage,
            Order.active.is_(True),
            Order.api_id == api_id,
        )
        .order_by(Order.created_at.asc())
        .limit(1)
        .with_for_update()
    )
    cand: Order | None = await session.scalar(candidate_q)

    if cand:
        cand.stage = stage
        await session.commit()
        # рекурсивно пытаемся подтянуть дальше
        await recursive_stage_update(api_id, stage + 1, reason)


@connection
async def update_stage_config(api_id: str, user_id: str, cfg: List[StageConfigModel], session: AsyncSession):
    api = await get_api_by_id(api_id)
    if api.user_id != user_id:
        return None
    stmt = delete(StageConfig).where(StageConfig.api_id == api_id)
    await session.execute(stmt)
    await session.flush()
    objs = []
    for c in cfg:
        obj = c.model_dump()
        objs.append(StageConfig(api_id = api_id, **obj))
    session.add_all(objs)
    await session.commit()
    return objs


@connection
async def update_settings(api_id: str, user_id: str, settings: List[SettingsModel], session: AsyncSession) -> Mapped[
                                                                                                                  list[ SymbolSettings]] | None:
    api = await get_api_by_id(api_id)
    api = await get_api_by_id(api_id)
    if api.user_id != user_id:
        return None
    sets = [sett.model_dump() for sett in settings]
    stmt = delete(SymbolSettings).filter(SymbolSettings.api_id == api_id)
    await session.execute(stmt)
    await session.commit()
    new_settings = [SymbolSettings(api_id=api_id, **s) for s in sets]
    # api.settings = new_settings
    # await session.commit()
    session.add_all(new_settings)
    await session.commit()

    return new_settings


@connection
async def get_settings_by_symbol(symbol: str, reason: ReasonEnum, api_id, session: AsyncSession) ->  SymbolSettings:
    stmt = select(SymbolSettings).where(SymbolSettings.symbol == symbol, SymbolSettings.active == True,
                                        SymbolSettings.api_id == api_id)
    res = await session.execute(stmt)
    return res.scalar()


@connection
async def get_signals_by_df(                       # noqa: C901
    df: pd.DataFrame,
    *,
    symbol: str,
    timeframe: str,
    rsi_len: Optional[int] = None,
    cci_len: Optional[int] = None,
    #api_id: Optional[str] = None,
    session: AsyncSession,
) -> List[Tuple[str, ReasonEnum]]:
    """
    Возвращает список (api_id, ReasonEnum) для сигналов текущего бара.
    Сигнал учитывает:
      • symbol
      • timeframe
      • длину RSI / CCI
      • включённость фильтров в настройках
    """

    out: List[Tuple[str, ReasonEnum]] = []

    # ───── 1. базовая валидация данных ─────
    if df.shape[0] < 2:
        return out
    last, prev = df.iloc[-1], df.iloc[-2]

    if (
        (rsi_len and (math.isnan(last.rsi) or math.isnan(prev.rsi))) or
        (cci_len and (math.isnan(last.cci) or math.isnan(prev.cci)))
    ):
        return out

    # ───── 2. выбираем настройки С УЧЁТОМ timeframe и длин ─────
    conds = [
        SymbolSettings.symbol == symbol,
        SymbolSettings.active.is_(True),
        SymbolSettings.timeframe == timeframe,
    ]
    stmt = select(distinct(SymbolSettings.delay_bars)).where(SymbolSettings.use_delay_filter == True)
    res = await session.execute(stmt)
    delays = res.scalars().all()
    delays = [0]+delays
    if rsi_len:
        conds.append(SymbolSettings.rsi_length == rsi_len)
        conds.append(SymbolSettings.use_rsi == True)
        conds.append(SymbolSettings.rsi_threshold >= last["rsi"])
        conds.append(SymbolSettings.rsi_threshold < prev["rsi"])
        col = "rsi"

    if cci_len:
        conds.append(SymbolSettings.cci_length == cci_len)
        conds.append(SymbolSettings.use_cci == True)
        conds.append(SymbolSettings.cci_threshold >= last["cci"])
        conds.append(SymbolSettings.cci_threshold < prev["cci"])
        col = "cci"
    #conds.append(0)

    #q = select(SymbolSettings).where(and_(*cond))

    for d in delays:
        if col == "cci":
            val = df.iloc[-1 - d]["cci"]
            stmt = select(SymbolSettings).where(
                    and_(*conds)
                )
        else:
            val = df.iloc[-1 - d]["rsi"]
            stmt = select(SymbolSettings).where(
                    and_(*conds,

                         or_(
                             and_(
                                 SymbolSettings.use_delta_filter == True,
                                 func.abs(SymbolSettings.delta_filter) > last["delta"]
                             ),
                             SymbolSettings.use_delta_filter == False
                         )
                         )
                )

        sett = (await session.execute(stmt)).scalars().all()
        signals_d = {
            "cci": ReasonEnum.RED,
            "rsi": ReasonEnum.YELLOW
        }
        reas = signals_d[col]
        for s in sett:
            #if symbol == 'DOGE-USDT' and timeframe == "1m":
            try:
                logger.info(f"{prev[col]} > {s.cci_threshold} >= {last['cci']} ({symbol})")
            except: pass
            can, stg = await can_open_extra(s.api_id, reas)
            if can:
                out.append((s.api_id, reas))

        return out


    # ───── 3. генерация сигналов ─────
    for sett in filter(None, settings_rows):
        # delay-filter
        if sett.use_delay_filter and sett.delay_bars > 0:
            try:
                t_last, t_prev = df["time"].iloc[-1], df["time"].iloc[-2]
            except KeyError:
                t_last = t_prev = None
            if t_last is not None and t_prev is not None:
                if (t_last - t_prev) < sett.delay_bars * 60_000:
                    continue

        # delta-filter
        if sett.use_delta_filter and sett.delta_filter != 0:
            if "delta" not in df.columns:
                continue
            d = last.delta
            if not (abs(d) >= sett.delta_filter if sett.delta_filter > 0 else d >= sett.delta_filter):
                continue

        # RSI ➜ YELLOW
        if rsi_len and sett.use_rsi and prev.rsi > sett.rsi_threshold >= last.rsi:
            can, stg = await can_open_extra(sett.api_id, ReasonEnum.YELLOW)
            if can:
                out.append((sett.api_id, ReasonEnum.YELLOW))
                logger.info(f"RSI signal {symbol} {timeframe} stage {stg}")

        # CCI ➜ RED
        if cci_len and sett.use_cci and prev.cci > sett.cci_threshold >= last.cci:
            can, stg = await can_open_extra(sett.api_id, ReasonEnum.RED)
            if can:
                out.append((sett.api_id, ReasonEnum.RED))
                logger.info(f"CCI signal {symbol} {timeframe} stage {stg}")

    return out

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


if __name__ == "__main__":
    asyncio.run(create_tables())
