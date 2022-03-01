from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
)
from database import Base


class Demographics(Base):
    __tablename__ = 'demographics'

    id = Column(Integer, primary_key=True)
    participant_identifier = Column(String)
    nhs_number = Column(String)
    uhl_system_number = Column(String)
    gp_practice = Column(String)
    age = Column(Integer)
    date_of_death = Column(Date)
    date_of_birth = Column(Date)
    postcode = Column(String)
    sex = Column(Integer)
    ethnic_category = Column(String)
