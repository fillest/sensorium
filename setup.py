import setuptools


setuptools.setup(
	name = 'sensorium',
	version = '0.24.2',
	packages = ['sensorium'],
	zip_safe = False,
	entry_points = {
		'console_scripts': ['sensorium = sensorium:main'],
	},
)
