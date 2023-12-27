from os import path

from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))
about = {}
with open(path.join(here, 'marshmallow_pyspark', 'version.py'), mode='r', encoding='utf-8') as f:
    exec(f.read(), about)

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

if __name__ == '__main__':
    setup(
        name='marshmallow-pyspark',
        description='PySpark data serializer',
        keywords='pyspark serializer marshmallow data-pipeline data-quality',
        version=about['__version__'],
        author='Ketan Goyal',
        author_email='ketangoyal1988@gmail.com',
        license="Apache 2.0 license",
        url='https://github.com/ketgo/marshmallow-pyspark',
        long_description=long_description,
        long_description_content_type='text/markdown',
        py_modules=['marshmallow_pyspark'],
        python_requires='>=3.4',
        install_requires=[
            'marshmallow~=3.2',
            'marshmallow-oneofschema~=3.0',
        ],
        extras_require={
            'dev': [
                'pyspark',
                'pytest~=4.0',
                'pytest-cov~=2.6',
                'pytest-spark~=0.5',
                'pylint~=1.0',
            ]
        },
        packages=find_packages(exclude=('tests', 'benchmarks')),
        classifiers=[
            'Intended Audience :: Developers',
            'License :: OSI Approved :: Apache Software License',
            'Operating System :: OS Independent',
            'Topic :: System :: Systems Administration',
            'Topic :: Software Development',
            'Topic :: Utilities',
            'Natural Language :: English',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: Implementation :: PyPy',
        ],
    )
