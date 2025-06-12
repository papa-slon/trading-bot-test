import asyncio
import os
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_session, create_async_engine, async_scoped_session, async_sessionmaker, \
    AsyncSession, AsyncAttrs
from sqlalchemy.orm import declarative_base, DeclarativeBase, Mapped, mapped_column, declared_attr
from sqlalchemy import URL, Integer, func, NullPool
from dotenv import load_dotenv

load_dotenv()

db = os.getenv("DATABASE")
host = os.getenv("HOST")
#port = os.getenv("PORT")
username = os.getenv("USERNAME_PG")
password = os.getenv("PASSWORD")

print(username)

url = URL.create(
    database=db,
    host=host,
    port=5432,
    username=username,
    password=password,
    drivername="postgresql+asyncpg",

)

print(url)

engine = create_async_engine(url=url, poolclass=NullPool)

SessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,          # ← гарантируем именно AsyncSession
    expire_on_commit=False        # ← не инвалидировать объекты после commit
)

class Base(AsyncAttrs, DeclarativeBase):
    __abstract__ = True
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    @declared_attr.directive
    def __tablename__(cls) -> str:

        return cls.__name__.lower() + 's'

async def get_session() -> AsyncSession:
    session = SessionLocal()
    try:
        yield session
    except SQLAlchemyError:
        await session.rollback()
        raise
    finally:
        await session.close()

def connection(method):
    async def wrapper(*args, **kwargs):
        async with SessionLocal() as session:
            loop = asyncio.get_event_loop()
            asyncio.set_event_loop(loop)
            await asyncio.sleep(0.02)
            try:
                # Явно не открываем транзакции, так как они уже есть в контексте
                return await method(*args, session=session, **kwargs)
            except Exception as e:
                await session.rollback()  # Откатываем сессию при ошибке
                print(e)
                await session.close()
                raise # Поднимаем исключение дальше
            finally:
                await session.close()  # Закрываем сессию

    return wrapper
