'use strict';

var q          = require('q'),
    config     = require('./config'),
    nano, // will be required in init

    /**
     * Extracts ids from a couchdb response and pushes them into
     * a supplied array.
     *
     * @param  {array} data
     * @return {array}
     */
    extractIds = function (data, fieldName) {
        var store = [],
            i;

        fieldName = fieldName || 'id';

        for (i = 0; i < data.length; i += 1) {
            store.push(data[i][fieldName] || data[i].value[fieldName]);
        }

        return store;
    },

    /**
     * Checks the config file for database name overrides and changes the dbName to the override.
     *
     * @param  {string} dbName The database name
     * @return {string}        The new database name
     */
    getDbName = function (dbName) {
        if (config.db.prefix) {
            dbName = config.db.prefix + dbName;
        }

        return dbName;
    },

    /**
     * Destroys a database.
     *
     * @param  {string} dbName The database name.
     * @return {promise} Resolves when the database is destroyed.
     */
    destroy = function (dbName) {
        var defer = q.defer();

        dbName = getDbName(dbName);

        console.log('Destroying db "' + dbName + '"');

        nano.db.destroy(dbName, function (error) {
            if (error) {
                defer.reject(error);
                console.log('error');
                return;
            }

            defer.resolve();

        });

        return defer.promise;
    },

    /**
     * Inserts one or many items into a database. Returns an 'into' function to apply the
     * insert to the supplied database. 'into' therefore is necessary every time you
     * call insert.
     *
     * @example
     * insert([{'name': 'jeff'}, {'name': 'Annie'}]).into('people');
     *
     * @todo Replace all with allSettled
     * @param  {object | array} items Must be JSON or an array of JSON.
     * @return {promiseAll}   Returns a q.all promise. If one insert fails
     *                                then all are discarded.
     */
    insert = function (items) {
        var keyName;

        if (!Array.isArray(items)) {
            items = [items];
        }

        return {
            'withKey': function (_keyName) {
                keyName = _keyName;

                return this;
            },

            'into': function (dbName) {
                var database,
                    index = 0,
                    inserting = [];

                dbName = getDbName(dbName);
                database = nano.use(dbName);

                items.forEach(function (item) {
                    var defer = q.defer(),
                        args = [
                            item,
                            function (error, body) {
                                if (error) {
                                    defer.reject(error);
                                    console.log(error);
                                    return;
                                }

                                defer.resolve(body);
                                index += 1;
                            }
                        ];

                    inserting.push(defer.promise);

                    if (keyName) {
                        args.splice(1, 0, keyName);
                    }

                    database.insert.apply(this, args);
                });

                // 'all' returns a single promise that will be resolved when the array
                // of promises that are supplied as it's parameter are resolved.
                return q.all(inserting);
            }
        };
    },

    /**
     * Wrapping the get functionality for the database.
     *
     * @param  {string} _id
     * @return {promise}
     */
    get = function (_id) {
        return {
            'from': function (dbName) {
                var defer = q.defer();

                dbName = getDbName(dbName);

                nano
                    .use(dbName)
                    .get(_id, function (error, body) {
                        if (error) {
                            defer.reject(error);
                            return;
                        }

                        defer.resolve(body);
                    });

                return defer.promise;
            }
        };
    },

    /**
     * Wrapping the list functionality for the database.
     *
     * @return {promise}
     */
    getList = function () {
        return {
            'from': function (dbName) {
                var defer = q.defer();

                dbName = getDbName(dbName);

                nano
                    .use(dbName)
                    .list(function (error, body) {
                        if (error) {
                            defer.reject(error);
                            return;
                        }

                        defer.resolve(body);
                    });

                return defer.promise;
            }
        };
    },

    /**
     * Creates a database with the name supplied as parameter.
     *
     * @param  {string} dbName The name for the database.
     * @return {promise}
     */
    create = function (dbName) {
        var creating = q.defer();

        dbName = getDbName(dbName);

        console.log('Creating db "' + dbName + '"');

        nano.db.create(dbName, function (error) {
            if (error) {
                creating.reject(error);
                return;
            }

            creating.resolve();
        });

        return creating.promise;
    },

    /**
     * Adds a view script to a db.
     *
     * @todo Make this accept multiple viewname: function key value pairs.
     * @todo Add error handling.
     *
     * @param {string} dbName
     * @param {string} designName
     * @param {object} viewMaps
     * @return {promise}
     */
    addDesign = function (designName, viewMaps) {
        return {
            'to': function (dbName) {
                var defer = q.defer(),
                    design = {
                        'language': 'javascript',
                        'views': {}
                    },
                    viewMap;

                dbName = getDbName(dbName);

                for (viewMap in viewMaps) {
                    if (viewMaps.hasOwnProperty(viewMap)) {
                        design.views[viewMap] = {
                            'map': viewMaps[viewMap].map
                        };

                        if (viewMaps[viewMap].reduce) {
                            design.views[viewMap].reduce = viewMaps[viewMap].reduce;
                        }
                    }
                }

                console.log('Adding design ' + designName);

                nano
                    .use(dbName)
                    .insert(design, '_design/' + designName, function (error, body/*, headers*/) {
                        if (!error) {
                            defer.resolve(body);

                        } else {
                            defer.reject(error);
                        }
                    });

                return defer.promise;
            }
        };
    },

    /**
     * Wraps nano to retrieve a list of the db names and check if the supplied name
     * is included in that list.
     *
     * @param  {string | array} dnName
     * @return {promise}    Resolves to a boolean.
     */
    checkDbExists = function (dbNames) {
        var dbList,
            promises = [],
            runChecks = function (defer, dbName) {
                defer.resolve(dbList.indexOf(dbName) !== -1);
            };

        if (!Array.isArray(dbNames)) {
            dbNames = [dbNames];
        }

        dbNames.forEach(function (dbName) {
            var defer = q.defer();

            promises.push(defer.promise);

            if (dbList) {
                runChecks(defer, dbName);

            } else {
                nano.db.list(function (error, _dbList) {
                    dbList = _dbList;
                    runChecks(defer, dbName);
                });
            }
        });

        return q.all(promises);
    },

    /**
     * Retrieve data from a couchdb view.
     *
     * @param  {string} dbName The database name.
     * @param  {string} designName The design name.
     * @param  {string} view   The view name.
     * @param  {object} params  Key value pairs to use as GET params in request.
     * @return {promise}    Resolves to the body returned by db.
     */
    getView = function (designName, viewName) {
        var args = [designName, viewName];

        return {
            'withParams': function (params) {
                // Params are conditional, but if they exist, they need to go
                // before the callback, so we splice them in after the viewName.
                if (params) {
                    args.push(params);
                }

                return this;
            },
            'from': function (dbName) {
                var defer = q.defer();

                dbName = getDbName(dbName);

                args.push(function (error, body) {
                    if (error) {
                        return defer.reject(error);
                    }

                    defer.resolve(body);
                });

                nano.use(dbName).view.apply(this, args);

                return defer.promise;
            }
        };
    },

    /**
     * Mark a revision as deleted in couchdb. Please read the apache docs to understand the inner
     * workings of a delete in couch, and reasons why delete is handled in a 'special' way.
     *
     * @see http://wiki.apache.org/couchdb/HTTP_Document_API#DELETE
     * @param  {string} revisionId The latest revision id of the document.
     * @return {promise}           Resolves to a success, failure or error message.
     */
    deleteDoc = function (docId) {

        return {
            'from': function (dbName) {
                var db = nano.use(dbName),
                    defer = q.defer();

                db.get(docId, { revs_info: true }, function (revError, revInfo) {
                    if (revError) {
                        return defer.reject(revError);
                    }

                    db.destroy(docId, revInfo._rev, function (error, body) {
                        if (error) {
                            return defer.reject(error);
                        }

                        defer.resolve(body);
                    });
                });

                return defer.promise;
            }
        };
    },

    init = function (options) {
        var dbLocation = options.url || 'http://localhost:5984';
        nano = require('nano')(dbLocation);
    };

exports.init = init;
exports.extractIds = extractIds;
exports.destroy = destroy;
exports.create = create;
exports.get = get;
exports.insert = insert;
exports.getView = getView;
exports.getList = getList;
exports.addDesign = addDesign;
exports.checkDbExists = checkDbExists;
exports.deleteDoc = deleteDoc;
