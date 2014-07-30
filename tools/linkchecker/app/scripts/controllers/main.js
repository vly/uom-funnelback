'use strict';

angular.module('linkcheckerApp')
    .controller('MainCtrl', function ($scope, $http, $templateCache) {
        $scope.method = 'GET';
        $scope.url = '';
        $scope.base = '/webstructure/api/v1.0/links?q=';
        $scope.loading = false;
        $scope.message = '';
        $scope.data = [];
        $scope.suggestions = [];

        $scope.fetch = function() {
            $scope.code = null;
            $scope.loading = true;
            $scope.response = null;
            $scope.message = '';
            $scope.data = [];
            $scope.suggestions = [];

            if($scope.url.indexOf('http') !== -1) {
                $scope.url = $scope.url.split('//', 2)[1];
            }

            if($scope.url.indexOf('www.') != -1) {
                $scope.url = $scope.url.split('www.', 2)[1];
            }

            $http({method: $scope.method, url: $scope.base + $scope.url, cache: $templateCache}).
                success(function(data, status) {
                    $scope.status = status;
                    if (data.size == 100) {
                        $scope.message = "Reached search limit, returning first 100 results";
                    } else if (data.size == 0) {
                        $scope.fetchSuggestions();
                        return;
                    }
                    $scope.loading = false;
                    $scope.data = data;

                    $scope.d2 = {};
                    for(var i=0;i<$scope.data['data']; i++) {
                        if (!($scope.data['data'][i].hostname in $scope.d2)) {
                            $scope.d2[$scope.data['data'][i].hostname] = {"data": [], "count": 0};
                        }
                        $scope.d2[$scope.data['data'][i].hostname]['data'].push($scope.data['data'][i]);
                        $scope.d2[$scope.data['data'][i].hostname]['count'] += 1;
                    }
                }).
                error(function(data, status) {
                    $scope.data = data || "Request failed";
                    $scope.message = 'Sorry, failed to retrieve the data.'
                    $scope.status = status;
                    $scope.loading = false;
                });
        };

        $scope.fetchSuggestions = function() {
            $scope.code = null;
            $scope.response = null;
            $scope.suggestions = [];
            $scope.suggestionBase = '/webstructure/api/v1.0/similarpages?q='

            $http({method: $scope.method, url: $scope.suggestionBase + $scope.url, cache: $templateCache}).
                success(function(data, status) {
                    $scope.loading = false;
                    if (data.size == 0) {
                        $scope.message = 'Sorry, no results found.';
                        return;
                    }
                    $scope.message = 'Sorry, we couldn\'t find the page you were looking for. Perhaps it\'s one of these?';
                    $scope.suggestions = data;

                }).
                error(function(data, status) {
                    $scope.suggestions = data || "Request failed";
                    $scope.loading = false;
                    $scope.message = 'Sorry, no results found.';
                });
        };

        $scope.updateModel = function(method, url) {
            $scope.method = method;
            $scope.url = $scope.base + url;
        };

        $scope.tocsv = function (objArray) {
            var array = typeof objArray != 'object' ? JSON.parse(objArray) : objArray;

            var str = '';
            var line = '';

            if ($("#labels").is(':checked')) {
                var head = array[0];
                if ($("#quote").is(':checked')) {
                    for (var j in array[0]) {
                        var value = j + "";
                        line += '"' + value.replace(/"/g, '""') + '",';
                    }
                } else {
                    for (var j in array[0]) {
                        line += j + ',';
                    }
                }

                line = line.slice(0, -1);
                str += line + '\r\n';
            }

            for (var i = 0; i < array.length; i++) {
                var line = '';

                if ($("#quote").is(':checked')) {
                    for (var j in array[i]) {
                        var value = array[i][j] + "";
                        line += '"' + value.replace(/"/g, '""') + '",';
                    }
                } else {
                    for (var j in array[i]) {
                        line += array[i][j] + ',';
                    }
                }

                line = line.slice(0, -1);
                str += line + '\r\n';
            }
            return str;
            
        };
                
            
        $scope.downloadcsv = function () {
            var csv = $scope.tocsv($scope.data.data);
            window.open("data:text/csv;charset=utf-8," + escape(csv));
        };

    });
