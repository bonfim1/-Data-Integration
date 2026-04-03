from datetime import datetime
from typing import Optional

from sqlalchemy import String, Numeric, SmallInteger, DateTime, ForeignKey, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Country(Base):
    __tablename__ = "countries"

    iso2_code:    Mapped[str]             = mapped_column(String(2),   primary_key=True)
    iso3_code:    Mapped[Optional[str]]   = mapped_column(String(3),   nullable=True)
    name:         Mapped[str]             = mapped_column(String(100), nullable=False)
    region:       Mapped[Optional[str]]   = mapped_column(String(80),  nullable=True)
    income_group: Mapped[Optional[str]]   = mapped_column(String(60),  nullable=True)
    capital:      Mapped[Optional[str]]   = mapped_column(String(80),  nullable=True)
    longitude:    Mapped[Optional[float]] = mapped_column(Numeric(9, 4),  nullable=True)
    latitude:     Mapped[Optional[float]] = mapped_column(Numeric(9, 4),  nullable=True)
    loaded_at:    Mapped[Optional[datetime]] = mapped_column(DateTime, default=datetime.now)

    facts = relationship("WdiFact", back_populates="country")


class Indicator(Base):
    __tablename__ = "indicators"

    indicator_code: Mapped[str]           = mapped_column(String(40), primary_key=True)
    indicator_name: Mapped[str]           = mapped_column(Text,       nullable=False)
    unit:           Mapped[Optional[str]] = mapped_column(String(30), nullable=True)

    facts = relationship("WdiFact", back_populates="indicator")


class WdiFact(Base):
    __tablename__ = "wdi_facts"

    iso2_code:      Mapped[str]             = mapped_column(String(2),  ForeignKey("countries.iso2_code"),       primary_key=True)
    indicator_code: Mapped[str]             = mapped_column(String(40), ForeignKey("indicators.indicator_code"), primary_key=True)
    year:           Mapped[int]             = mapped_column(SmallInteger, primary_key=True)
    value:          Mapped[Optional[float]] = mapped_column(Numeric(18, 4), nullable=True)
    loaded_at:      Mapped[Optional[datetime]] = mapped_column(DateTime, default=datetime.now)

    country   = relationship("Country",   back_populates="facts")
    indicator = relationship("Indicator", back_populates="facts")