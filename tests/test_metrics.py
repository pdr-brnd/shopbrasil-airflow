import pytest

from include.metrics import compute_category_metrics, extract_categories

PRODUCTS = [
    {"id": 1, "title": "a", "price": 10.0, "category": "x"},
    {"id": 2, "title": "b", "price": 20.5, "category": "x"},
    {"id": 3, "title": "c", "price": 5.0, "category": "y"},
]


def test_categories_sorted_distinct():
    assert extract_categories(PRODUCTS) == ["x", "y"]


def test_metrics():
    assert compute_category_metrics(PRODUCTS, "x") == {
        "category": "x", "product_count": 2, "avg_price": 15.25, "min_price": 10.0, "max_price": 20.5,
    }


def test_unknown_category():
    with pytest.raises(ValueError):
        compute_category_metrics(PRODUCTS, "z")
