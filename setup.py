from setuptools import setup, find_packages
from srm import __version__ as version

install_requires = []
try:
    import eventlet
except ImportError:
    install_requires.append("eventlet")

name = "swift-ring-master"

data_files = [('share/swift-ring-master',
               ['README.md',
                'etc/swift/ring-master.conf-sample',
                'etc/swift/ring-minion.conf-sample'])]

if not os.getenv('VIRTUAL_ENV', False):
    data_files.append(('/etc/init.d', ['etc/init.d/swift-ring-minion']))

setup(
    name = name,
    version = version + "-rax3",
    author = "Florian Hines",
    author_email = "syn@ronin.io",
    description = "Manage swift rings",
    license = "Apache License, (2.0)",
    keywords = "openstack swift",
    url = "http://github.com/pandemicsyn/ring-master",
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Environment :: No Input/Output (Daemon)',
        ],
    install_requires=install_requires,
    scripts=['bin/swift-ring-master-server',
             'bin/swift-ring-master-wsgi-server',
             'bin/swift-ring-minion-server'],
    data_files = data_files)
