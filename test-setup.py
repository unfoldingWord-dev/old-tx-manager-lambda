from setuptools import setup

setup(
    name="tx-manager",
    version="0.0.1",
    author="unfoldingWord",
    author_email="unfoldingword.org",
    description="Unit test setup file.",
    keywords="",
    url="https://github.org/unfoldingWord-dev/tx-manager",
    packages=['tx_manager'],
    long_description='Unit test setup file',
    classifiers=[],
    dependency_links=[
                'git+git://github.com/richmahn/tx-manager.git#egg=tx-manager',
    ],
    install_requires=[
        'tx-manager'
    ],
    test_suite='tests'
)
