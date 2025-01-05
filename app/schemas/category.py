from typing import Dict, List, Optional, Set
from pydantic import BaseModel

class CategoryNode(BaseModel):
    """Node in the category hierarchy."""
    name: str
    parent: Optional[str] = None
    children: Set[str] = set()
    platform_mappings: Dict[str, List[str]] = {}

class CategoryHierarchy(BaseModel):
    """Represents a hierarchical category structure."""
    nodes: Dict[str, CategoryNode] = {}
    
    def add_category(self, name: str, parent: Optional[str] = None) -> None:
        """Add a category to the hierarchy."""
        if name not in self.nodes:
            self.nodes[name] = CategoryNode(name=name, parent=parent)
            
        if parent and parent in self.nodes:
            self.nodes[parent].children.add(name)
            self.nodes[name].parent = parent
            
    def get_ancestors(self, category: str) -> List[str]:
        """Get all ancestors of a category."""
        ancestors = []
        current = self.nodes.get(category)
        
        while current and current.parent:
            ancestors.append(current.parent)
            current = self.nodes.get(current.parent)
            
        return ancestors
        
    def get_descendants(self, category: str) -> List[str]:
        """Get all descendants of a category."""
        descendants = []
        to_process = list(self.nodes[category].children)
        
        while to_process:
            child = to_process.pop()
            descendants.append(child)
            to_process.extend(self.nodes[child].children)
            
        return descendants
        
    def map_platform_category(self, platform: str, category: str) -> List[str]:
        """Map a platform-specific category to internal categories."""
        mapped = []
        for node in self.nodes.values():
            if platform in node.platform_mappings:
                if category in node.platform_mappings[platform]:
                    mapped.append(node.name)
                    mapped.extend(self.get_ancestors(node.name))
        return list(set(mapped)) 