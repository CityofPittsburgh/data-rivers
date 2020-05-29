import setuptools

REQUIRED_PACKAGES = [
    # 'usaddress-scourgify==0.1.11',
    'google-cloud-storage==1.28.1',
]

setuptools.setup(
    name='dataflow_utils',
    version='0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)