/**
 * Charts service
 * Handle graph displaying
 */
var chartsService = function($q, $rootScope, rpcService) {
    var self = this;
    
    /**
     * Get graph data for specified device
     */
    self.getDeviceData = function(uuid, timestampStart, timestampEnd, options) {
        return rpcService.sendCommand('get_data', 'charts', {'uuid':uuid, 'timestamp_start':timestampStart, 'timestamp_end':timestampEnd, 'options':options});
    };

};
    
var RaspIot = angular.module('RaspIot');
RaspIot.service('chartsService', ['$q', '$rootScope', 'rpcService', chartsService]);

