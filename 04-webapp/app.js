'use strict';
const http = require('http');
var assert = require('assert');
const express = require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4]) })

function removePrefix(text, prefix) {
	if (text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}

function groupByMovies(userid, cells) {
	let result = [];
	let movieDetails;
	let lastMovieId = 0; // No Movie Yet
	cells.forEach(function (cell) {
		let MovieId = Number(removePrefix(cell['key'], userid + '_'));
		if (lastMovieId !== MovieId) {
			if (movieDetails) {
				result.push(movieDetails)
			}
			movieDetails = { "userid": userid, "movieid": MovieId.toString() }
		}
		movieDetails[removePrefix(cell['column'], 'reco:')] = cell['$']
		lastMovieId = MovieId;
	})
	return result;

}

app.use(express.static('public'));
app.get('/movierecos.html', function (req, res) {
	const userid = req.query['userid'];
	console.log(userid);
	hclient.table('vigneshv_all_users_top_movie_reco').scan(
		{
			filter: {
				type: "PrefixFilter",
				value: userid + '_'
			},
			maxVersions: 1
		},
		function (err, cells) {
			let template = filesystem.readFileSync("result.mustache").toString();
			let html = mustache.render(template, { userid, movie_details: groupByMovies(userid, cells) });
			res.send(html);
		})
});

app.listen(port);