from setuptools import find_packages, setup

CLASSIFIERS = [
    'License :: OSI Approved :: BSD License',
    'Framework :: Django',
    "Operating System :: POSIX :: Linux",
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Framework :: Django :: 2.2',
    'Framework :: Django :: 3.0',
]

setup(
    name='django-cosmos',
    version='0.0.1',
    description='Django backend for Azure Cosmos DB',
    long_description=open('README.md').read(),
    author='Anthony Shaw',
    author_email='anthonyshaw@apache.org',
    url='https://github.com/tonybaloney/django-cosmos',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'azure-cosmos',
    ],
    classifiers=CLASSIFIERS,
    keywords='django',
)