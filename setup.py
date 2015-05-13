from setuptools import setup, find_packages


setup(
    name='mixpanel-query-py',
    version=__import__('mixpanel_query').__version__,
    description='The Python interface to query data from Mixpanel.',
    author='Sean Coonce, Francis Genet, Patrick McNally, Harry Marr',
    author_email='cooncesean@gmail.com',
    url='https://www.github.com/cooncesean/mixpanel-query-py',
    packages=find_packages(),
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    include_package_data=True,
    zip_safe=False,
)
