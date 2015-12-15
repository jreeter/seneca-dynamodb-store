'use strict';

var _ = require('lodash');
var AWS = require('aws-sdk');
var attributes = require('dynamodb-data-types');
var uuid = require('node-uuid');

module.exports = function(options) {
	
	var seneca = this;
	var name = 'dynamadb-store';
	var dynamoContext = null;
	
	//interface to dynamodb/seneca.store
	var store = {
		name: name,
		close: function(args, done) {
			throw new Error('Close not implemented.');
		},
		save: function(args, done) {
			
			var entity = args.ent.data$(false);
			var table = getTableName(entity);
			var serializedEntity;
			
			if(entity.id) {
				//update
				serializedEntity = serializeEntity(entity);
				
				var params = {
					TableName: table,
					Item: serializedEntity
				};
				
				dynamoContext.DocumentClient().put(params, function(err, data) {
					if(err) {
						done(err);
					} else {
						done(null, data);
					}
				})
				
			} else {
				//new entity
				entity.id = uuid();
				serializedEntity = serializeEntity(entity);
				
			}
		},
		load: function(args, done) {
			throw new Error('Load not implemented.');
		},
		list: function(args, done) {
			throw new Error('List not implemented.');
		},
		remove: function(args, done) {
			throw new Error('Remove not implemented.');
		},
		native: function(args, done) {
			done(dynamoContext);
		}
	};
	
	/**
	 * Returns the name portion of an entities zone/base/name, this will be used as the table name.
	 * @params entity
	 * @return table name 
	 */
	function getTableName(entity) {
		return entity.canon$({object:true}).name;
	}
	
	/**
	 * Serialize a seneca entity to a structure accepted by dynamo.
	 * @params entity
	 * @return table name 
	 */
	function serializeEntity(entity) {
		return attributes.wrap(entity);
	}
	
	/**
	 * Deserialize a dynamo respond to a seneca entity.
	 * @params entity
	 * @return table name 
	 */
	function deserializeEntity(entity) {
		
	}
	
	//configure database context
	function configure(options, cb) {
		dynamoContext = AWS.DynamoDB(options); //we should version lock this per AWS docs.
		if(dynamoContext) {
			cb();
		} else {
			cb('DynamoDb connection failure');
		}
	};
	
	//initialiaze dynamo config
	var meta = seneca.store.init(seneca, options, store);
	seneca.add({init:store.name, tag:meta.tag, function(args, done) {
		configure(options, function(err) {
			if(err) {
				return seneca.fail({code: 'dynamodb-store/configure', store: store.name, error: err}, done);	
			} else {
				done();
			}
		})
	}});

};
