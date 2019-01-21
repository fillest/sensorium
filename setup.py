import setuptools


setuptools.setup(
	name = 'sensorium',
	version = '0.24.3',
	packages = ['sensorium'],
	zip_safe = False,
	entry_points = {
		'console_scripts': ['sensorium = sensorium:main'],
	},
)
