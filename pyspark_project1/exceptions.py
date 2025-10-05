class ShippingLogisticsException(Exception):
    """Base exception for shipping logistics pipeline"""
    pass

class DataValidationError(ShippingLogisticsException):
    """Raised when data validation fails"""
    pass

class TransformationError(ShippingLogisticsException):
    """Raised when transformation fails"""
    pass

class ConnectionError(ShippingLogisticsException):
    """Raised when connection to external systems fails"""
    pass