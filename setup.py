from distutils.core import setup

def readme():
    try:
        with open('README.md') as f:
            return f.read()
    except IOError:
        return ''


setup(
    name='MySpark',
    version='0.0.1',
    author='Phillip Henry',
    author_email='londonjavaman@gmail.com',
    packages=['myspark'],
    url='http://github.com/PhillHenry/PySparkPlayground',
    license='LICENSE.txt',
    description='Me playing around with PySpark.',
    long_description=readme(),
)
