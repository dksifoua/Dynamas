from src.data_extraction.utils import encode

class FilterType:
    SINGLE_COLUMN_VALUE_FILTER = 'SingleColumnValueFilter'
    ROW_FILTER = 'RowFilter'
    

class Operator:
    EQUAL = 'EQUAL'
    

class Comparator:
    BINARY_COMPARATOR = 'BinaryComparator'
    

class NbCol:
    STOCK = 1
    RAWTWEETS = 10
    

class Category:
    ACTION = encode(b'Action')
    FINANCIAL = encode(b'Financial')
    TREND = encode(b'Trend')
    

class Action:
    AAPL = encode(b'AAPL')
    MSFT = encode(b'MSFT')