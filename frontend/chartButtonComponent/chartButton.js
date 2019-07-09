/**
 * Chart button
 * Display a button that opens chartdialog
 *
 * Directive example:
 * <div chart-button device="<device>" options="<options>"></div
 * @param device: device object
 * @param options: chart options. An object with the following format:
 *  {
 *      'type': <'bar', 'line'> : type of chart (string) (mandatory)
 *      'filters': ['fieldname1', ...]: list of field names to display (array) (optional)
 *      'timerange': { (optional)
 *          'start': <timestamp>: start range timestamp (integer)
 *          'end': <timestamp>: end range timestamp (integer)
 *      }
 *  }
 */
var chartButtonDirective = function($q, $rootScope, chartsService, $mdDialog, toast) {

    var chartButtonController = ['$scope', function($scope) {
        var self = this;
        self.buttonLabel = '';
        self.buttonClass = 'md-fab md-mini';

        /**
         * Cancel dialog
         */
        self.cancelDialog = function() {
            $mdDialog.cancel();
        };

        /**
         * Open chart dialog
         */
        self.openChartDialog = function() {
            $mdDialog.show({
                controller: function() { return self; },
                controllerAs: 'chartButtonCtl',
                templateUrl: 'chartButton/chartDialog.html',
                parent: angular.element(document.body),
                clickOutsideToClose: true,
                fullscreen: true,
                escapeToClose: false //disable esc key to avoid tooltip issue
            });
        };
    
    }];

    var chartButtonLink = function(scope, element, attrs, controller) {
        controller.device = scope.device;
        controller.chartOptions = scope.chartOptions;
        if( !angular.isUndefined(scope.buttonLabel) )
        {
            controller.buttonLabel = scope.buttonLabel;
        }
        if( !angular.isUndefined(scope.buttonClass) )
        {
            controller.buttonClass = scope.buttonClass;
        }
    };

    return {
        restrict: 'AE',
        templateUrl: 'chartButton/chartButton.html',
        replace: true,
        scope: {
            device: '=',
            chartOptions: '=chartOptions',
            buttonLabel: '@',
            buttonClass: '@'
        },
        controller: chartButtonController,
        controllerAs: 'chartButtonCtl',
        link: chartButtonLink
    };

};
    
var RaspIot = angular.module('RaspIot');
RaspIot.directive('chartButton', ['$q', '$rootScope', 'chartsService', '$mdDialog', 'toastService', chartButtonDirective]);

