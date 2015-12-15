'use strict';

var _ = require('lodash');
var AWS = require('aws-sdk');
var attributes = require('dynamodb-data-types').AttributeValue;
var uuid = require('node-uuid');

module.exports = function(opts) {
	var seneca = this;
	var dynamoContext = null;
	
	//interface to dynamodb/seneca.store
	var store = {
		name: 'dynamodb-store',
		save: function(args, done) {
			var table = getTableName(args.ent);
			
			//if has an id we need to update, else, create new entity
			if(args.ent.id) {
				saveExistingEntity(args.ent, table, done);
			} else {
				saveNewEntity(args.ent, table, done);
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
		close: function(args, done) {
			throw new Error('Close not implemented.');
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
	 * Serialize a seneca entity to a structure accepted by dynamo and removes seneca attributes via .data$.
	 * @params entity
	 * @return table name 
	 */
	function serializeEntity(entity) {
		return attributes.wrap(entity.data$(false));
	}
	
	/**
	 * Deserialize a dynamo respond to a seneca entity.
	 * @params entity
	 * @return table name 
	 */
	function deserializeEntity(entity) {
		return attributes.unwrap(entity);
	}
	
	//configure database context
	function configure(options, cb) {
		AWS.config.update(options);
	    dynamoContext = new AWS.DynamoDB(); //we should version lock this per AWS docs.
		if(dynamoContext) {
			cb();
	 	} else {
		 	cb('DynamoDb connection failure');
	 	}
	};
	
	//initialiaze dynamo 
	var meta = seneca.store.init(seneca, opts, store);
	seneca.add({init:store.name, tag:meta.tag}, function(args, done) {
		configure(opts, function(err) {
			if(err) {
				return seneca.fail({code: 'dynamodb-store/configure', store: store.name, error: err}, done);	
			} else {
				done();
			}
		})
	});
	
	/**
	 * Saves a new entity using aws-sdk.
	 * @params entity
	 * @params table
	 * @params done
	 * @return 
	 */
	function saveNewEntity(entity, table, done) {
		entity.id = uuid();
		var leanEntity =  serializeEntity(entity);
		
		var params = {
			TableName: table,
			Item: leanEntity
		};
		
		dynamoContext.putItem(params, function(err, data) {
			if(err) {
				done(err);
			} else {
				done(null, entity); 
			}
		})
	}
	
	/**
	 * Saves an existing entity using aws-sdk.
	 * @params entity
	 * @params table
	 * @params done
	 * @return 
	 */
	function saveExistingEntity(entity, table, done) {
		var leanEntity =  serializeEntity(entity);
		
		//create AttributeUpdates object.
		var updates = {};
		_.forEach(leanEntity, function(value, key) {
			if(key !== 'id') {
				updates[key] = {
					Action: 'PUT',
					Value: value
				};
			}
		});
		
		var params = {
			Key: {
				id: leanEntity.id
			},
			TableName: table,
			AttributeUpdates:  updates
		};
		
		dynamoContext.updateItem(params, function(err, data) {
			if(err) {
				done(err);
			} else {
				done(null, entity);
			}
		});
	}
	
	return {name: store.name, tag: meta.tag};
};
