from typing import Optional
from pydantic import BaseModel, Field

class PaginationParams(BaseModel):
    """Parameters for pagination and sorting."""
    
    page: int = Field(default=1, ge=1, description="Page number (1-based)")
    page_size: int = Field(default=20, ge=1, le=100, description="Number of items per page")
    sort_by: Optional[str] = Field(
        default='start_time',
        description="Field to sort by (start_time, popularity, price)"
    )
    sort_desc: bool = Field(default=True, description="Sort in descending order")
    
    @property
    def offset(self) -> int:
        """Calculate offset based on page and page_size."""
        return (self.page - 1) * self.page_size
    
    @property
    def limit(self) -> int:
        """Get limit (page_size)."""
        return self.page_size
    
    def get_pagination_info(self, total_count: int) -> dict:
        """
        Get pagination metadata.
        
        Args:
            total_count: Total number of items
            
        Returns:
            Dictionary with pagination metadata
        """
        total_pages = (total_count + self.page_size - 1) // self.page_size
        
        return {
            'page': self.page,
            'page_size': self.page_size,
            'total_pages': total_pages,
            'total_count': total_count,
            'has_next': self.page < total_pages,
            'has_prev': self.page > 1
        } 