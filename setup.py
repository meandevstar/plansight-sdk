from setuptools import setup, find_packages

setup(
    name='plansight_sdk',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'setuptools>=58.0.0',
        'pytest>=6.0.0',
    ],
    entry_points={
        'console_scripts': [],
    },
    package_data={
        'plansight_sdk': ['schema/schema.sql'],
    },
    author='meandevstar',
    description='A simple database client SDK supporting user and activity CURD operations',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/meandevstar/plansight_sdk',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
