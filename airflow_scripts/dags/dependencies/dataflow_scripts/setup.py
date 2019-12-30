import setuptools

REQUIRED_PACKAGES = ['usaddress-scourgify', 'google-cloud-storage']

setuptools.setup(
    name='dataflow_utils',
    version='0.0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)