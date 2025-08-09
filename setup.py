"""
Wheel Preparation for the Module
"""
from setuptools import (
    setup,
    find_packages,
)

setup(
    name='data_stream_processor',
    version='0.0.1',
    long_description=open("README.md", "r").read(),
    long_description_content_type='text/markdown',
    package_dir={'': '.'},
    include_package_data=False,
    packages=find_packages(
        where='.',
        include=['*.*'],
        exclude=['*test*']
    ),
    entry_points={
        'console_scripts': [
            'run-processor = src.main.processor.executor:main'
        ]
    },
    url='',
    author='rajdeep-chakraborty-418',
    author_email='rajdeepchakraborty728@gmail.com',
    description='data_stream_processor_wheel',
    extras_require={'dev': ['pytest']},
    python_requires='>=3.10',
    setup_requires=['pytest-runner', 'wheel'],
    test_suite="test"
)
