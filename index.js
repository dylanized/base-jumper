module.exports = function(config) {

	var mysql = require("mysql"),
		knex = require("knex")({client: "mysql"}),
		test = require("unit.js");
		
	var module = {};
	
	// db setup
	
		var connection = mysql.createConnection(config);					 
		connection.connect();
		
	// inserters
	
		module.insertRow = function(table, props, done) {
		
			// build query
			var query = knex(table).insert(props).toString();
			              
			//run query		  
			connection.query(query, function(err, rows, fields) {
			
				return done(err, rows);
			  	
			});		
		
		}
		
	// getters
	
		// Get Num Tables
		
		module.getNumTables = function(done) {

			var sql = "SELECT COUNT(DISTINCT `table_name`) FROM `information_schema`.`columns` WHERE `table_schema` = '" + config.database +"'";
						
			// query db for number of tables
			connection.query(sql, function(err, rows, fields) {
			
				if (err) done(err);
				
				else {
				
					var key = "COUNT(DISTINCT `table_name`)";
					
					if (rows && rows[0] && rows[0][key] !== undefined) done (null, rows[0][key]);
							
				}
				
			});
		
		}
	
		// Get Rows 
					
		module.getRows = function(table, props, done) {
		
			// build query
			var query = knex(table).select("*").where(props).toString();
			              
			//run query		  
			connection.query(query, function(err, rows, fields) {
			
				return done(err, rows);
			  	
			});
		
		}
		
		// Get Single Row 
					
		module.getRow = function(table, props, done) {
		
			// build query
			var query = knex(table).select("*").where(props).toString();
			              
			//run query		  
			connection.query(query, function(err, rows, fields) {

				// if row found, return it
				if (rows && rows[0]) done(err, rows[0]);
			
				// else return err
				else done(err);
			  	
			});
		
		}
		
		// Get Rows By Ascending
					
		module.getRowsAsc = function(table, props, orderby, done) {
		
			// if order is array
		
			// build query
			var query = knex(table).select("*").where(props).orderBy(orderby, "asc").toString();
			              
			//run query		  
			connection.query(query, function(err, rows, fields) {
			
				return done(err, rows);
			  	
			});
		
		}	
		
		// Get Rows By Descending
					
		module.getRowsDesc = function(table, props, orderby, done) {
		
			// if order is array
		
			// build query
			var query = knex(table).select("*").where(props).orderBy(orderby, "desc").toString();
			              
			//run query		  
			connection.query(query, function(err, rows, fields) {
			
				return done(err, rows);
			  	
			});
		
		}			
		
		// Get Num Rows 
					
		module.getNumRows = function(table, props, done) {
		
			module.getRows(table, props, function(err, rows) {
			
				var num = false;
				
				if (rows && rows.length != undefined) num = rows.length;
				
				done(num);
			
			});		
		
		}
		
	// checkers

		// Check Number of Tables
		
		module.checkNumTables = function(num_tables, done) {
		
			module.getNumTables(function(err, num) {
			
				if (err) test.fail(err);
				
				else test.number(num).is(num_tables);
				
				return done();					
			
			});
			
		}
	
		// Check Num Rows
		
		module.checkNumRows = function(table, props, num_rows, done) {
		
			module.getRows(table, props, function(err, rows) {
			
				if (err) test.fail(err);
				
			  	test.array(rows).hasProperty("length", num_rows);
			  	
			  	return done();
			  					
			});				
		
		}		
	
	return module;
	
}