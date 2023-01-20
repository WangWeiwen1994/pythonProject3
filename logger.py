import logging


logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
handler = logging.FileHandler("log/log.txt")

handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(message)s')

handler.setFormatter(formatter)

console = logging.StreamHandler()

console.setLevel(logging.INFO)

logger.addHandler(handler)

logger.addHandler(console)
