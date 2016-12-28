const fs = require('fs')
const parse = require('csv-parse')
const async = require('async')
const mongodb = require('mongodb')
const { MongoClient } = mongodb

const trips = './csv/trips.txt'
const shapes = './csv/shapes.txt'
const url = 'mongodb://localhost:27017/busRoutes'
let test = 0

class BusRoutesCsvParser {
	init() {
		this.shapes = {}

		MongoClient.connect(url, (err, db) => {
			if (err) {
				console.log('mongo connection error', err)
				return
			}

			let collection = db.collection('trips')

			var count = collection.count().then((rows) => {
				if (rows === 0)
					this.update()
				else {
					console.log('DB HAS ITEMS')
					this.readShapes()
				}
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
						row = { latitude: rowData[1], longitude: rowData[2] }

						this.organizeShapes(rowData[0], row).then(() => {
							if (lineNumber === length) {
								let counter = 0

								for (let prop in this.shapes) {
									// console.log(this.shapes[prop].coordinates.length)
									this.queue(counter, () => {
										this.insertData('shapes', this.shapes[prop])							
									})

									counter++
								}

								resolve()
							}

							callback()
						})

						lineNumber++
						
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
					// console.log('mongo connection error')
					resolve()
					return
				}

				let collection = db.collection(collectionName)

				collection.insert(data)

				db.close()
				resolve()
			})
		})
	}

	updateShapes(id, coordinates) {
		return new Promise((resolve, reject) => {
			MongoClient.connect(url, (err, db) => {
				if (err) {
					console.log('mongo connection error', err)
					resolve()
					return
				}

				let collection = db.collection('shapes')

				collection.update({ id }, { $set: { coordinates } })

				db.close()
				resolve()
			})
		})
	}

	organizeShapes(id, row) {
		return new Promise((resolve, reject) => {
			if (typeof this.shapes[id] === 'undefined') {
				this.shapes[id] = { id, coordinates: [ row ] }
				// this.insertData('shapes', this.shapes[id]).then(() => {
				// })

				resolve()					
				return
			}

			this.shapes[id].coordinates.push(row)
			resolve()
		})
	}

	queue(i, cb) {
		let time = i * 25
		console.log(time);

		setTimeout(() => {
			cb()
		}, time)
	}
}

var routes = new BusRoutesCsvParser()

routes.init()