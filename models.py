
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import (DeclarativeBase,
                            Mapped,
                            mapped_column,
                            )
from sqlalchemy.types import String,Text

# Set Connection URI to PostgreSQL
ps_uri = 'postgresql://postgres:postgres@localhost:5435/qwork'

# Create the Postgres engine
ps_engine = create_engine(ps_uri )


# Inherit DeclarativeBase to Base for simplicity
class Base(DeclarativeBase):
    pass

# Create KeyboardMechDL model to handle keyboard_mech_dl table
class KeyboardMechDL(Base):
    __tablename__ = 'keyboard_mech_dl'
    
    id: Mapped[int] = mapped_column(primary_key = True,
                                    autoincrement = True)
    title: Mapped[str] = mapped_column(String(100))
    description: Mapped[str] = mapped_column(Text)

# Create KeyboardMechDWH model to handle keyboard_mech_dwh table
class KeyBoardMechDWH(Base):
    __tablename__ = 'keyboard_mech_dwh'
    
    dwh_id : Mapped[int] = mapped_column(primary_key = True,
                                   autoincrement= True)
    id: Mapped[int]
    title: Mapped[str] = mapped_column(String(100),nullable = True)	
    material: Mapped[str]= mapped_column(String(20),nullable = True)	
    shape_profile : Mapped[str]= mapped_column(String(20),nullable = True)	
    keys: Mapped[int] = mapped_column(nullable = True)

# Create tables if not exists or connect the tables
Base.metadata.create_all(ps_engine)