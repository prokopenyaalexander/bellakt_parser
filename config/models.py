from datetime import datetime
from sqlalchemy import String, Integer, DateTime, Float
from sqlalchemy.orm import Session, sessionmaker, DeclarativeBase, Mapped, mapped_column
from config.orm_core import engine


class Base(DeclarativeBase):
    pass

class SiteSet(Base):
    __tablename__ = "siteset_orm"

    id: Mapped[int] = mapped_column(Integer, primary_key= True)
    name: Mapped[String] = mapped_column(String(1024), nullable=True)
    url: Mapped[String] = mapped_column(String(4096), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.date, nullable=True)

class PricingProducts(Base):
    __tablename__ = "pricing_products_orm"
    id: Mapped[int] = mapped_column(Integer, primary_key= True)
    sku: Mapped[int] = mapped_column(Integer, nullable=True)
    name: Mapped[String] = mapped_column(String(1024), nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=True)
    stock: Mapped[String] = mapped_column(String(32), nullable=True)
    product_url: Mapped[String] = mapped_column(String(4096), nullable=True)
    date: Mapped[datetime] = mapped_column(DateTime, default=datetime.date, nullable=True)

class ProductContent(Base):
    __tablename__ = "product_content_orm"
    id: Mapped[int] = mapped_column(Integer, primary_key= True)
    sku: Mapped[int] = mapped_column(Integer, nullable=True)
    title: Mapped[String] = mapped_column(String, nullable=True)
    number_of_images: Mapped[int] = mapped_column(Integer, nullable=True)
    best_before_date: Mapped[String] = mapped_column(String(32), nullable=True)
    characteristic: Mapped[String] = mapped_column(String, nullable=True)
    date: Mapped[datetime] = mapped_column(DateTime, default=datetime.date, nullable=True)


class RankingProducts(Base):
    __tablename__ = "ranking_products_orm"
    id: Mapped[int] = mapped_column(Integer, primary_key= True)
    category_name: Mapped[String] = mapped_column(String(4096), nullable=True)
    count_of_products: Mapped[int] = mapped_column(Integer, nullable=True)
    category_url: Mapped[String] = mapped_column(String, nullable=True)
    date: Mapped[datetime] = mapped_column(DateTime, default=datetime.date, nullable=True)


class UrlsToCrawling(Base):
    __tablename__ = "urls_to_crawling_orm"
    id: Mapped[int] = mapped_column(Integer, primary_key= True)
    pricing_url: Mapped[String] = mapped_column(String, nullable=True)
    category_url: Mapped[String] = mapped_column(String, nullable=True)
    date: Mapped[datetime] = mapped_column(DateTime, default=datetime.date, nullable=True)



SiteSet.metadata.create_all(engine)