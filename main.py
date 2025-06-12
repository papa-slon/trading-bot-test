import logging
import sys
import configparser
from gui.app_gui import BotGUI

def setup_logging():
    config = configparser.ConfigParser()
    config.read('config.ini')

    logfile = config.get("LOGGING", "logfile", fallback="logs/bot.log")
    loglevel = config.get("LOGGING", "loglevel", fallback="INFO")

    logging.basicConfig(
        level=getattr(logging, loglevel.upper(), logging.INFO),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.FileHandler(logfile, mode='a', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def run_app():

    window = BotGUI()
    window.start_bot()

def main():
    setup_logging()
    run_app()

if __name__ == "__main__":
    main()
