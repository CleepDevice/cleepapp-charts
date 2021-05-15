/**
 * Chart directive
 * Display chart of specified device values
 *
 * Directive example:
 * <chart device="<device>" options="<options>"></div>
 *
 * @param device: device object
 * @param options: chart options. An object with the following format
 *  {
 *    type (string)       : Type of chart (optional, default line) (line|bar|pie)
 *    fields (array)      : List of field names to display (optional, default all fields)
 *    timerange (obj)     : Timerange to display at opening (optional, default last day until now)
 *                          {
 *                              predefined (int): use pre-defined range. Possible values:
 *                                  - 86400   : last day
 *                                  - 172800  : last 2 days
 *                                  - 604800  : last week
 *                                  - 1209600 : last 2 weeks
 *                                  - 2678400 : last month
 *                                  - 7862400 : last quarter
 *                                  - 15724800: last semester
 *                                  - 31449600: last year
 *                              start (timestamp): start range timestamp
 *                              end (timestamp)  : end range timestamp
 *                          }
 *    format (callback)   : Callback to convert value to specific format (optional, default is raw value) 
 *                          Format infos available here https://github.com/d3/d3-format
 *    height (int)        : Chart height (optional, default 400px)
 *    color (string|array): Color hex code (starting with #). Can be an array of colors for series charts.
 *    loadData (callback) : Callback that returns data to display (mandatory for pie chart).
 *                          Callback parameters:
 *                              - start (timestamp): start timestamp
 *                              - end (timestamp)  : end timestamp
 *                          Returns: callback must return a promise. Output value must follow a specific format
 *                          according to list or dict format.
 *    showControls (bool) : Display or not controls (time range...) (optional, default is true)
 *    label (string)      : Chart vertical label (generally the unit),
 *    title (string)      : Top chart title
 *  }
 */
angular
.module('Cleep')
.directive('chart', ['chartsService', 'toastService',
function(chartsService, toast) {

    var chartController = ['$scope', function($scope) {
        var self = this;
        self.device = null;
        self.options = null;
        self.loading = true;
        self.chartHeight = '400px';
        self.rangeSelector = 86400;
        self.rangeStart = 0;
        self.rangeEnd = 0;
        self.timestampStart = 0;
        self.timestampEnd = 0;
        self.showControls = true;

        // dynamic time format according to zoom
        /*self.customTimeFormat = d3.time.format.multi([
            ["%H:%M", function(d) { return d.getMinutes(); }], 
            ["%H", function(d) { return d.getHours(); }], 
            ["%a %d", function(d) { return d.getDay() && d.getDate() != 1; }], 
            ["%b %d", function(d) { return d.getDate() != 1; }], 
            ["%B", function(d) { return d.getMonth(); }], 
            ["%Y", function() { return true; }]
        ]);*/
        self.customTimeFormat = d3.time.format.multi([
            ["%m/%d/%y %H:%M", function(d) { return true; }], 
        ]);

        // bar chart default options
        // http://krispo.github.io/angular-nvd3/#/historicalBarChart
        self.historicalBarChartOptions = {
            chart: {
                type: "historicalBarChart",
                height: 400,
                margin: {
                    top: 20,
                    right: 20,
                    bottom: 65,
                    left: 50
                },
                x: function(d){ return d[0]; },
                y: function(d){ return d[1]; },
                showValues: true,
                duration: 500,
                xAxis: {
                    // axisLabel: "X Axis",
                    // rotateLabels: 30,
                    showMaxMin: false,
                    tickFormat: function(d) {
                        return self.customTimeFormat(moment(d,'X').toDate());
                    },
                    scale: d3.time.scale()
                },
                yAxis: {
                    axisLabel: '',
                    axisLabelDistance: -15,
                    tickFormat: function(v) {
                        return self.defaultFormat(v);
                    }
                },
                tooltip: {
                    keyFormatter: function(d) {
                        return self.customTimeFormat(moment(d,'X').toDate());
                    }
                },
                zoom: {
                    enabled: true,
                    scaleExtent: [1,10],
                    useFixedDomain: false,
                    useNiceScale: false,
                    horizontalOff: false,
                    verticalOff: true,
                    unzoomEventType: "dblclick.zoom"
                }
            },
            title: {
                enable: false,
                text: ''
            }
        };

        // multibar chart default options
        // http://krispo.github.io/angular-nvd3/#/multiBarChart
        self.multiBarChartOptions = {
            chart: {
                type: 'multiBarChart',
                height: 400,
                margin : {
                	top: 20,
                    right: 20,
                    bottom: 45,
                    left: 45
                },
                clipEdge: true,
                duration: 500,
                stacked: true,
                xAxis: {
                    // axisLabel: 'Time (ms)',
                    showMaxMin: false,
                    tickFormat: function(d) {
                        return self.customTimeFormat(moment(d,'X').toDate());
                    }
                },
                yAxis: {
                    axisLabel: '',
                    axisLabelDistance: -15,
                    tickFormat: function(v) {
                        return self.defaultFormat(v);
                    }
                },
                zoom: {
                    enabled: true,
                    scaleExtent: [1,10],
                    useFixedDomain: false,
                    useNiceScale: false,
                    horizontalOff: false,
                    verticalOff: true,
                    unzoomEventType: "dblclick.zoom"
                }
            },
            title: {
                enable: false,
                text: '',
            }
        };

        // line chart default options
        // http://krispo.github.io/angular-nvd3/#/stackedAreaChart
        self.stackedAreaChartOptions = {
            chart: {
                type: 'stackedAreaChart',
                height: 400,
                margin : {
                    top: 20,
                    right: 20,
                    bottom: 30,
                    left: 40
                },
                x: function(d){return d[0];},
                y: function(d){return d[1];},
                useVoronoi: false,
                clipEdge: true,
                duration: 100,
                useInteractiveGuideline: true,
                xAxis: {
                    showMaxMin: false,
                    tickFormat: function(d) {
                        return self.customTimeFormat(moment(d,'X').toDate());
                    }
                },
                yAxis: {
                    axisLabel: '',
                    axisLabelDistance: -15,
                    tickFormat: function(v) {
                        return self.defaultFormat(v);
                    }
                },
                zoom: {
                    enabled: true,
                    scaleExtent: [1, 10],
                    useFixedDomain: false,
                    useNiceScale: false,
                    horizontalOff: false,
                    verticalOff: true,
                    unzoomEventType: 'dblclick.zoom'
                },
                showControls: false,
                showLegend: false
            },
            title: {
                enable: false,
                text: ''
            }
        };

        // pie chart default options
        // http://krispo.github.io/angular-nvd3/#/pieChart
        self.pieChartOptions = {
            chart: {
                type: "pieChart",
                height: 400,
                showLabels: true,
                duration: 500,
                labelThreshold: 0.05,
                labelType: 'percent',
                donut: true,
                donutRatio: 0.35,
                x: function(d) {
                    return d.key;
                },
                y: function(d) {
                    return self.defaultFormat(d.value);
                },
                legend: {
                    margin: {
                        top: 5,
                        right: 35,
                        bottom: 5,
                        left: 0
                    }
                }
            },
            title: {
                enable: false,
                text: ''
            }
        };

        // default value format callback
        self.defaultFormat = function(v) {
            return v;
        };

        // chart types<=>options mapping
        self.chartOptionsByType = {
            'line': self.stackedAreaChartOptions,
            'bar': self.historicalBarChartOptions,
            'pie': self.pieChartOptions,
            'multibar': self.multiBarChartOptions,
        };

        // chart data and options
        self.chartData = [];
        self.chartOptions = {};

        // data for chart values request
        self.chartRequestOptions = {
            output: 'list',
            fields: [],
            sort: 'ASC'
        };

        /**
         * Prepare chart options according to directive options
         */
        self.__prepareChartOptions = function() {
            // set chart request options and chart options
            if( !angular.isUndefined(self.options) && self.options!==null ) {
                // chart type
                if( !angular.isUndefined(self.options.type) && self.options.type!==null ) {
                    self.chartOptions = self.chartOptionsByType[self.options.type];
                    switch(self.options.type) {
                        case 'line':
                            self.chartRequestOptions.output = 'list';
                            break;
                        case 'bar':
                            self.chartRequestOptions.output = 'list';
                            break;
                        case 'multibar':
                            self.chartRequestOptions.output = 'dict';
                            break;
                        case 'pie':
                            self.chartRequestOptions.output = 'dict';
                            break;
                        default:
                            // invalid type specified
                            toast.error('Invalid chart type specified');
                            return;
                    }
                }

                // force chart height
                if( !angular.isUndefined(self.options.height) && self.options.height!==null ) {
                    self.chartOptions.chart.height = self.options.height;
                    self.chartHeight = '' + self.options.height + 'px';
                }

                // fields filtering
                if( !angular.isUndefined(self.options.fields) && self.options.fields!==null ) {
                    self.chartRequestOptions.fields = self.options.fields;
                }

                // force values format
                if( !angular.isUndefined(self.options.format) && self.options.format!==null ) {
                    self.defaultFormat = self.options.format;
                }

                // force Y label
                if( !angular.isUndefined(self.options.label) && self.options.label!==null ) {
                    self.chartOptions.chart.yAxis.axisLabel = self.options.label;
                    self.chartOptions.chart.margin.left = 60;
                }

                // force title
                if( !angular.isUndefined(self.options.title) && self.options.title!==null ) {
                    self.chartOptions.title.enable = true;
                    self.chartOptions.title.text = self.options.title;
                }

                // force color
                if( !angular.isUndefined(self.options.color) && self.options.color!==null ) {
                    if( angular.isArray(self.options.color) ) {
                        self.chartOptions.chart.color = self.options.color;
                    } else {
                        self.chartOptions.chart.color = [self.options.color];
                    }
                }
            }
        };

        /**
         * Finalize chart options according to directive options
         * @param data: data to parse for charting
         */
        self.__finalizeChartOptions = function(data) {
            var chartData = [];
            var count = 0;
            var name = null;
            
            switch(self.options.type) {
                case 'line':
                    chartData = self.__computeLineChartValues(data);
                    break;
                case 'bar':
                    chartData = self.__computeBarChartValues(data);
                    break;
                case 'multibar':
                    chartData = self.__computeMultiBarChartValues(data);
                    break;
                case 'pie':
                    chartData = self.__computePieChartValues(data);
                    break;
            }

            // display legend only if there are some values (except for pie chart)
            if( chartData.length>1 && self.options.type!=='pie' )
            {
                self.chartOptions.chart.showLegend = true;
                self.chartOptions.chart.margin.top = 30;
            }   

            // set chart data and loading flag
            self.chartData = chartData;
            self.loading = false;
        };
        
        /**
         * Compute line chart values
         */
        self.__computeLineChartValues = function(data) {
            // Output format
            //  {
            //    key: "Serie1",
            //    values: [[timestamp, value], ...]
            //  }
            var values = [];
            
            for( var name in data ) {
                values.push({
                    'key': data[name].name,
                    'values': data[name].values
                });
            }
            
            return values;
        };
        
        /**
         * Compute bar chart values
         */
        self.__computeBarChartValues = function(data) {
            // Output format
            //  {
            //    key: "Serie1",
            //    bar: true,
            //    values: [[timestamp, value], ...]
            //  }
            var values = [];
            
            for( var name in data ) {
                values.push({
                    'key': name,
                    'bar': true,
                    'values': data[name].values
                });
            }
            
            return values;
        };

        /**
         * Compute multibar chart values
         */
        self.__computeMultiBarChartValues = function(data) {
            // Output format:
            //  [
            //    {                                 {                               {
            //      key: "Stream0",                   key: "Stream1",                 key: "Stream2",
            //      values: [                         values: [                       values: [
            //        {                                 {                               {
            //          x: 0                              x: 0                            x: 0
            //          y: 1.0902505986887638             y: 0.9494608038493945           y: 0.9863569449592955
            //          y0: 0                             y0: 1.0902505986887638          y0: 2.0397114025381584
            //          series: 0                         series: 1                       series: 2
            //          key: "Stream0"                    key: "Stream1"                  key: "Stream2"
            //          size: 1.0902505986887638          size: 0.9494608038493945        size: 0.9863569449592955
            //          y1: 1.0902505986887638            y1: 2.0397114025381584          y1: 3.026068347497454
            //        },                                },                              },
            //        ...                               ...                             ...
            //      ]                                 ]                               ]
            //    },                                },                              },
            //  ]
            var values = [];

            var series = [];
            for( var name in data[0] ) {
                if( name!=='ts' ) {
                    series.push(name);
                    values.push({
                        key: name,
                        values: [],
                    });
                }
            }

            for( var value of data ) {
                var index = 0;
                for( var serie of series ) {
                    var lastValue = {
                        x: value.ts,
                        y: value[serie],
                        y0: index===0 ? 0 : lastValue.y + lastValue.y0,
                        series: index,
                        key: serie,
                        size: value[serie],
                        y1: value[serie] + (index===0 ? 0 : lastValue.y + lastValue.y0),
                    };
                    values[index].values.push(lastValue);
                    index++;
                }
            }

            return values;
        };
        
        /**
         * Compute pie chart values
         */
        self.__computePieChartValues = function(data) {
            // Output format:
            //  [
            //    { key: "stream1", value: 0 },
            //    ...
            //  ]
            var values = [];
            
            for( name in data ) {
                values.push({
                    'key': data[name].name,
                    'value': data[name].value
                });
            }
            
            return values;
        }
        
        /**
         * Load chart data
         */
        self.loadChartData = function(scope, el) {
            // set loading flag
            self.loading = true;

            // prepare chart options
            self.__prepareChartOptions();

            // load chart data
            if( !angular.isUndefined(self.options.loadData) && self.options.loadData!==null )
            {
                // load user data
                self.options.loadData(self.timestampStart, self.timestampEnd)
                    .then(function(resp) {
                        self.__finalizeChartOptions(resp);
                    })
                    .catch(function(error) {
                        // unable to get data, stop loading
                        self.loading = false;
                    });
            }
            else
            {
                // load device data
                chartsService.getDeviceData(self.device.uuid, self.timestampStart, self.timestampEnd, self.chartRequestOptions)
                    .then(function(resp) {
                        self.__finalizeChartOptions(resp.data.data);
                    })
                    .catch(function(error) {
                        // unable to get data, stop loading
                        self.loading = false;
                    });
            }
        };

        /**
         * Change time range
         */
        self.changeRange = function()
        {
            // compute new timestamp range
            self.timestampEnd = Number(moment().format('X'));
            self.timestampStart = self.timestampEnd - self.rangeSelector;

            // load new chart data
            self.loadChartData();
        };

        /**
         * Init controller
         */
        self.init = function()
        {
            // force user timestamp if provided
            if( !angular.isUndefined(self.options.timerange) && self.options.timerange!==null ) {
                if( !angular.isUndefined(self.options.timerange.predefined) && self.options.timerange.predefined!==null ) {
                    // use predefined timerange
                    self.rangeSelector = self.options.timerange.predefined;
                    self.timestampEnd = Number(moment().format('X'));
                    self.timestampStart = self.timestampEnd - self.rangeSelector;
                } else {
                    // use custom timerange
                    self.rangeSelector = 0;
                    self.timestampStart = self.options.timerange.start;
                    self.timestampEnd = self.options.timerange.end;
                }
            } else {
                // set default timestamp range
                self.timestampEnd = Number(moment().format('X'));
                self.timestampStart = self.timestampEnd - self.rangeSelector;
            }

            // show controls
            if( !angular.isUndefined(self.options.showControls) ) {
                self.showControls = self.options.showControls;
            }

            // load chart data
            self.loadChartData();
        };

        /**
         * Destroy directive
         */
        $scope.$on('$destroy', function() {
            // workaround to remove tooltips when dialog is closed: dialog is closed before 
            // nvd3 has time to remove tooltips elements
            var tooltips = $("div[id^='nvtooltip']");
            for( var i=0; i<tooltips.length; i++ ) {
                tooltips[i].remove();
            }
        });

    }];

    var chartLink = function(scope, element, attrs, controller) {
        controller.device = scope.device;
        controller.options = scope.options;
        controller.init();
    };

    return {
        restrict: 'AE',
        templateUrl: 'chartComponent/chart.html',
        replace: true,
        scope: {
            device: '=',
            options: '=options',
        },
        controller: chartController,
        controllerAs: 'chartCtl',
        link: chartLink
    };

}]);

