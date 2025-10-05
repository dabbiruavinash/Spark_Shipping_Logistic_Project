import re
from typing import List

class ValidationUtils:
    @staticmethod
    def validate_email(email: str) -> bool:
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email)) if email else False
    
    @staticmethod
    def validate_phone(phone: str) -> bool:
        pattern = r'^\+?[\d\s\-\(\)]{10,}$'
        return bool(re.match(pattern, phone)) if phone else False
    
    @staticmethod
    def validate_postal_code(postal_code: str, country: str = 'US') -> bool:
        patterns = {
            'US': r'^\d{5}(-\d{4})?$',
            'CA': r'^[A-Z]\d[A-Z] \d[A-Z]\d$'
        }
        pattern = patterns.get(country, r'.*')
        return bool(re.match(pattern, postal_code)) if postal_code else False