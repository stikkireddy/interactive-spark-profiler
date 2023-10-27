from setuptools import setup, find_packages

reqs = open("requirements.txt").read().split("\n")

setup(
    name='interactive-spark-profiler',
    author='Sri Tikkireddy',
    author_email='sri.tikkireddy@databricks.com',
    description='A for interactively profiling spark code in notebooks',
    packages=find_packages(exclude=['notebooks']),
    setup_requires=['setuptools_scm'],
    install_requires=reqs,
    license_files=('LICENSE',),
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    keywords='Databricks Clusters',
)
