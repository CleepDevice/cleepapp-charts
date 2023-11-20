/**
 * Charts service
 * Handle graph displaying
 */
angular
.module('Cleep')
.service('chartsService', ['rpcService',
function(rpcService) {
    var self = this;
    
    /**
     * Get graph data for specified device
     */
    self.getDeviceData = function(uuid, timestampStart, timestampEnd, options) {
        return rpcService.sendCommand('get_data', 'charts', {'device_uuid':uuid, 'timestamp_start':timestampStart, 'timestamp_end':timestampEnd, 'options':options});
    };

}]);
    
