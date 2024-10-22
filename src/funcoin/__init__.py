import logging

logger = logging.getLogger("funcoin")
logger.setLevel(level=logging.INFO)
logger.addHandler(logging.StreamHandler())
logger.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
