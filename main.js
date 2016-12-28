const fs = require('fs')
const parse = require('csv-parse')
const async = require('async')
const mongodb = require('mongodb')
const { MongoClient } = mongodb

const trips = './csv/trips.txt'
const shapes = './csv/shapes.txt'
const url = 'mongodb://localhost:27017/busRoutes'

class BusRoutesCsvParser {
	init() {
		MongoClient.connect(url, (err, db) => {
			if (err) {
				console.log('mongo connection error', err)
				return
			}

			let collection = db.collection('trips')

			var count = collection.count().then((rows) => {
				if (rows === 0)
					this.update()
				else
					console.log('DB HAS ITEMS')
			})

			db.close()
		})
	}

	update() {
		this.readTrips()
			.then(this.readShapes.bind(this))
			.then(() => {
				console.log('pego a porra toda')
			})
	}

	readTrips() {
		return new Promise((resolve, reject) => {
			let lineNumber = 0

			let parser = parse({ delimiter: ',' }, (err, data) => {
				let length = data.length
				console.log('READING TRIPS DATA FROM CSV FILE...')

				async.eachSeries(data, (rowData, callback) => {
					let row = {}

					if (lineNumber > 1) {
						row = {
							route: rowData[0],
							service: rowData[1],
							trip: rowData[2],
							name: rowData[3],
							direction: rowData[4],
							shape: rowData[5]
						}
						
						this.insertData('trips', row).then(() => {
							if (lineNumber === length)
								resolve()

							callback()
						})

						lineNumber++
					} else {
						lineNumber++

						callback()
					}


				})
			})
			
			fs.createReadStream(trips).pipe(parser)
		})
	}

	readShapes() {
		return new Promise((resolve, reject) => {
			let lineNumber = 0

			let parser = parse({ delimiter: ',' }, (err, data) => {
				let length = data.length
				console.log('READING SHAPE DATA FROM CSV FILE...')

				async.eachSeries(data, (rowData, callback) => {
					let row = {}

					if (lineNumber > 1) {
						row = {
							id: rowData[0],
							coordinates: [
								rowData[1], rowData[2]
							],
							sequence: rowData[3],
							dist: rowData[4]
						}

						this.insertData('shapes', row).then(() => {
							if (lineNumber === length)
								resolve()

							lineNumber++
							callback()
						})
					} else {
						lineNumber++
						callback()
					}
				})
			})
			
			fs.createReadStream(shapes).pipe(parser)
		})
	}

	insertData(collectionName, data) {
		return new Promise((resolve, reject) => {
			MongoClient.connect(url, (err, db) => {
				if (err) {
					console.log('mongo connection error')
					return
				}

				let collection = db.collection(collectionName)

				collection.insert(data)

				db.close()
				resolve()
			})
		})
	}
}

var routes = new BusRoutesCsvParser()

routes.init()