from app.db.session import Base, engine

def init_db():
    """Initialize database tables"""
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db() 