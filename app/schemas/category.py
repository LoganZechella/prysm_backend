from typing import Dict, List, Set, Optional
from pydantic import BaseModel

class CategoryNode:
    """Node in the category hierarchy."""
    def __init__(self, name: str, parent: Optional[str] = None, 
                 keywords: List[str] = None, platform_mappings: Dict[str, List[str]] = None):
        self.name = name
        self.parent = parent
        self.children: Set[str] = set()
        self.keywords = keywords or []
        self.platform_mappings = platform_mappings or {}

class CategoryHierarchy:
    """Hierarchical category structure."""
    def __init__(self):
        self.nodes: Dict[str, CategoryNode] = {}
        
    def add_category(self, name: str, parent: Optional[str] = None, 
                    keywords: List[str] = None, platform_mappings: Dict[str, List[str]] = None):
        """Add a category to the hierarchy."""
        if name in self.nodes:
            return
            
        node = CategoryNode(name, parent, keywords, platform_mappings)
        self.nodes[name] = node
        
        if parent:
            if parent not in self.nodes:
                self.add_category(parent)
            self.nodes[parent].children.add(name)
            
    def get_ancestors(self, category: str) -> List[str]:
        """Get all ancestors of a category in order from root to leaf."""
        if category not in self.nodes:
            return []
            
        ancestors = []
        current = category
        while self.nodes[current].parent:
            current = self.nodes[current].parent
            ancestors.insert(0, current)  # Insert at beginning to maintain root-to-leaf order
            
        return ancestors
        
    def get_descendants(self, category: str) -> Set[str]:
        """Get all descendants of a category."""
        if category not in self.nodes:
            return set()
            
        descendants = set()
        to_process = list(self.nodes[category].children)
        
        while to_process:
            child = to_process.pop()
            descendants.add(child)
            to_process.extend(self.nodes[child].children)
            
        return descendants
        
    def map_platform_category(self, platform: str, category: str) -> Optional[str]:
        """Map a platform-specific category to our hierarchy."""
        # Try direct mapping first
        for name, node in self.nodes.items():
            if platform in node.platform_mappings:
                if category in node.platform_mappings[platform]:
                    return name
                    
        # Try keyword matching
        category_lower = category.lower()
        for name, node in self.nodes.items():
            if any(keyword.lower() in category_lower for keyword in node.keywords):
                return name
                
        return None 