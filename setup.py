from setuptools import setup

setup(
    name='mysqlstmt',
    version='1.0.1',
    url='https://github.com/lovette/mysqlstmt',
    download_url='https://github.com/lovette/mysqlstmt/archive/master.tar.gz',
    license='BSD',
    author='Lance Lovette',
    author_email='lance.lovette@gmail.com',
    description='Python library to build SQL statements for MySQL.',
    long_description=open('README.md').read(),
    packages=['mysqlstmt'],
    install_requires=[],
    tests_require=['nose'],
    zip_safe=False,
    platforms='any',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
