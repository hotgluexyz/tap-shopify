#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-shopify",
    version="1.4.14",
    description="Singer.io tap for extracting Shopify data",
    author="Stitch",
    url="http://github.com/singer-io/tap-shopify",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    python_requires='>=3.5.2',
    py_modules=["tap_shopify"],
    install_requires=[
        "ShopifyAPI==12.7.0",
        "singer-python==5.12.1",
        "requests==2.29.0"
    ],
    extras_require={
        'dev': [
            'pylint==2.7.4',
            'ipdb',
            'requests==2.20.0',
            'nose',
        ]
    },
    entry_points="""
    [console_scripts]
    tap-shopify=tap_shopify:main
    """,
    packages=[
        "tap_shopify",
        "tap_shopify.streams",
        "tap_shopify.streams.compatibility",
        "tap_shopify.streams.compatibility.value_maps",
    ],
    package_data={
        "tap_shopify": [
            "schemas/*.json",
            "streams/compatibility/value_maps/*.json",
        ]
    },
    include_package_data=True,
)
