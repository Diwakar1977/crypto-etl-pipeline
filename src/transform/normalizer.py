from src.utils.logger import get_logger

logger = get_logger(__name__)

def normalize(data):
    try:
        logger.info("Starting data normalization.")
        
        if data is None:
            logger.warning("Input data is None.")
            return[]
        
        if not isinstance(data, list):
            raise TypeError(f"Expected list, got {type(data).__name__}")
        
        cleaned = []

        for row in data:
            if not isinstance(row, dict):
                logger.warning(f"Skipping invalid row type: {type(row).__name__}")
                continue
            new_row = {}

            for k, v in row.items():
                if isinstance(v, (int, float)):
                    new_row[k] = float(v)

                elif v is None:
                    new_row[k] = None

                else:
                    new_row[k] = v
            
            cleaned.append(new_row)

        logger.info(f"Normalization completed. processed {len(cleaned)} records.")
        
        return cleaned
    
    except Exception as e:
        logger.exception(f"Normaliztion failed: {e}")
        raise
    
    