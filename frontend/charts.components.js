angular.module('Cleep').component('chartButton', {
    template: `
    <md-button ng-click="$ctrl.openChartDialog()" class="{{ $ctrl.btnStyle }}">
        <cl-icon cl-icon="{{ $ctrl.btnIcon }}"></cl-icon>
        <md-tooltip ng-if="$ctrl.clTooltip">{{ $ctrl.clTooltip }}</md-tooltip>
        <span ng-if="$ctrl.btnLabel.length">{{ $ctrl.btnLabel }}</span>
    </md-button>
    `,
    bindings: {
        clDevice: '<',
        clOptions: '<',
        clBtnLabel: '@?',
        clBtnIcon: '@?',
        clBtnStyle: '@?',
        clTooltip: '@?',
    },  
    controller: function ($mdDialog) {
        const ctrl = this;
        ctrl.btnLabel = 'Charts';
        ctrl.btnIcon = 'chart-line';
        ctrl.btnStyle = 'md-raised md-primary';

        ctrl.$onChanges = function (changes) {
            if (changes.clBtnLabel?.currentValue != null) {
                ctrl.btnLabel = changes.clBtnLabel.currentValue ?? 'Charts';
            }   
            if (changes.clBtnIcon?.currentValue != null) {
                ctrl.btnIcon = changes.clBtnIcon.currentValue || 'chart-line';
            }   
            if (changes.clBtnStyle?.currentValue != null) {
                ctrl.btnStyle = changes.clBtnStyle.currentValue || 'md-raised md-primary';
            }
        };  

        ctrl.cancelDialog = function() {
            $mdDialog.cancel();
        };  

        ctrl.openChartDialog = function() {
            $mdDialog.show({
                controller: function () { return ctrl; },
                controllerAs: '$ctrl',
                template: `
                <md-dialog flex="50">
                    <form ng-cloak>
                        <md-toolbar>
                            <div class="md-toolbar-tools">
                                <h2>{{ ::$ctrl.clDevice.name || 'Chart' }}</h2>
                                <span flex></span>
                                <md-button class="md-icon-button" ng-click="$ctrl.cancelDialog()">
                                    <cl-icon cl-icon="close"></cl-icon>
                                </md-button>
                            </div>
                        </md-toolbar>
                        <md-dialog-content>
                            <md-content layout-padding>
                                <chart device="$ctrl.clDevice" options="$ctrl.clOptions"></chart>
                            </md-content>
                        </md-dialog-content>
                    </form>
                </md-dialog>
                `,
                parent: angular.element(document.body),
                clickOutsideToClose: true,
                fullscreen: true,
            }); 
        };
    },
});

angular.module('Cleep').component('chart', {
    template: `
    <div layout="column" layout-align="center stretch">

        <!-- progress -->
        <div ng-if="$ctrl.loading" layout="row" layout-align="space-around center" ng-style="{'height':$ctrl.chartHeight}">
            <md-progress-circular md-mode="indeterminate"></md-progress-circular>
        </div>

        <!-- chart -->
        <div ng-if="!$ctrl.loading">
            <nvd3 options='$ctrl.chartOptions' data='$ctrl.chartData'></nvd3>
        </div>

        <!-- controls -->
        <div layout="row" layout-align="center center" ng-if="$ctrl.showControls">
            <div flex></div>

            <!-- information label -->
            <div style="padding-bottom:6px; padding-right:15px;">
                <span>Show data from:</span>
            </div>

            <!-- custom range -->
            <div ng-if="$ctrl.rangeSelector===0">
                <input ng-model="$ctrl.rangeStart">
                <input ng-model="$ctrl.rangeEnd">
            </div>

            <!-- pre-defined ranges -->
            <div>
                <md-select class="md-no-underline" ng-model="$ctrl.rangeSelector">
                    <md-option value="86400">last day</md-option>
                    <md-option value="172800">last 2 days</md-option>
                    <md-option value="604800">last week</md-option>
                    <md-option value="1209600">last 2 weeks</md-option>
                    <md-option value="2678400">last month</md-option>
                    <md-option value="7862400">last quarter</md-option>
                    <md-option value="15724800">last semester</md-option>
                    <md-option value="31449600">last year</md-option>
                    <md-option value="0" ng-disabled="true">Custom</md-option>
                </md-select>
            </div>

            <!-- button -->
            <div>
                <md-button class="md-raised md-primary button-raised-icon" ng-click="$ctrl.changeRange()">
                    Ok
                </md-button>
            <div>

        </div>

    </div>
    `,
    bindings: {
        device: '<',
        options: '<',
    },
    controller: function(chartsService, $scope) {
        const ctrl = this;
        ctrl.device = null;
        ctrl.options = null;
        ctrl.loading = true;
        ctrl.chartHeight = '400px';
        ctrl.rangeSelector = 86400;
        ctrl.rangeStart = 0;
        ctrl.rangeEnd = 0;
        ctrl.timestampStart = 0;
        ctrl.timestampEnd = 0;
        ctrl.showControls = true;

        // dynamic time format according to zoom
        /*ctrl.customTimeFormat = d3.time.format.multi([
            ["%H:%M", function(d) { return d.getMinutes(); }], 
            ["%H", function(d) { return d.getHours(); }], 
            ["%a %d", function(d) { return d.getDay() && d.getDate() != 1; }], 
            ["%b %d", function(d) { return d.getDate() != 1; }], 
            ["%B", function(d) { return d.getMonth(); }], 
            ["%Y", function() { return true; }]
        ]);*/
        ctrl.customTimeFormat = d3.time.format.multi([
            ["%m/%d/%y %H:%M", function(d) { return true; }], 
        ]);

        // bar chart default options
        // http://krispo.github.io/angular-nvd3/#/historicalBarChart
        ctrl.historicalBarChartOptions = {
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
                        return ctrl.customTimeFormat(moment(d,'X').toDate());
                    },
                    scale: d3.time.scale()
                },
                yAxis: {
                    axisLabel: '',
                    axisLabelDistance: -15,
                    tickFormat: function(v) {
                        return ctrl.format(v);
                    }
                },
                tooltip: {
                    keyFormatter: function(d) {
                        return ctrl.customTimeFormat(moment(d,'X').toDate());
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
        ctrl.multiBarChartOptions = {
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
                        return ctrl.customTimeFormat(moment(d,'X').toDate());
                    }
                },
                yAxis: {
                    axisLabel: '',
                    axisLabelDistance: -15,
                    tickFormat: function(v) {
                        return ctrl.format(v);
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
        ctrl.stackedAreaChartOptions = {
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
                        return ctrl.customTimeFormat(moment(d,'X').toDate());
                    }
                },
                yAxis: {
                    axisLabel: '',
                    axisLabelDistance: -15,
                    tickFormat: function(v) {
                        return ctrl.format(v);
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
        ctrl.pieChartOptions = {
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
                    return ctrl.format(d.value);
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

        // chart types<=>options mapping
        ctrl.chartOptionsByType = {
            'line': ctrl.stackedAreaChartOptions,
            'bar': ctrl.historicalBarChartOptions,
            'pie': ctrl.pieChartOptions,
            'multibar': ctrl.multiBarChartOptions,
        };

        // chart data and options
        ctrl.chartData = [];
        ctrl.chartOptions = {};

        // data for chart values request
        ctrl.chartRequestOptions = {
            output: 'list',
            fields: [],
            sort: 'ASC'
        };

        ctrl.$onInit = function () {
            // force user timestamp if provided
            if (!angular.isUndefined(ctrl.options.timerange) && ctrl.options.timerange!==null) {
                if  (!angular.isUndefined(ctrl.options.timerange.predefined) && ctrl.options.timerange.predefined!==null) {
                    // use predefined timerange
                    ctrl.rangeSelector = ctrl.options.timerange.predefined;
                    ctrl.timestampEnd = Number(moment().format('X'));
                    ctrl.timestampStart = ctrl.timestampEnd - ctrl.rangeSelector;
                } else {
                    // use custom timerange
                    ctrl.rangeSelector = 0;
                    ctrl.timestampStart = ctrl.options.timerange.start;
                    ctrl.timestampEnd = ctrl.options.timerange.end;
                }
            } else {
                // set default timestamp range
                ctrl.timestampEnd = Number(moment().format('X'));
                ctrl.timestampStart = ctrl.timestampEnd - ctrl.rangeSelector;
            }

            // show controls
            if( !angular.isUndefined(ctrl.options.showControls) ) {
                ctrl.showControls = ctrl.options.showControls;
            }

            // load chart data
            ctrl.loadChartData();
        };

        ctrl.$onDestroy = function () {
            // workaround to remove tooltips when dialog is closed: dialog is closed before 
            // nvd3 has time to remove tooltips elements
            const tooltips = $("div[id^='nvtooltip']");
            for (let i=0; i<tooltips.length; i++) {
                tooltips[i].remove();
            }
        };

        // default value format callback
        ctrl.format = function(value) {
            switch (ctrl.options?.format?.func) {
                case 'round':
                    if (ctrl.options?.format?.eval) {
                        value = $scope.$eval(ctrl.options?.format?.eval, { value })
                    }
                    return Math.round(value);
                default:
                    return value;
            }
        };

        /**
         * Prepare chart options according to directive options
         */
        ctrl.__prepareChartOptions = function() {
            // set chart request options and chart options
            if (!angular.isUndefined(ctrl.options) && ctrl.options!==null) {
                // chart type
                if (!angular.isUndefined(ctrl.options.type) && ctrl.options.type!==null) {
                    ctrl.chartOptions = ctrl.chartOptionsByType[ctrl.options.type];
                    switch (ctrl.options.type) {
                        case 'line':
                            ctrl.chartRequestOptions.output = 'list';
                            break;
                        case 'bar':
                            ctrl.chartRequestOptions.output = 'list';
                            break;
                        case 'multibar':
                            ctrl.chartRequestOptions.output = 'dict';
                            break;
                        case 'pie':
                            ctrl.chartRequestOptions.output = 'dict';
                            break;
                        default:
                            // invalid type specified
                            toast.error('Invalid chart type specified');
                            return;
                    }
                }

                // force chart height
                if (!angular.isUndefined(ctrl.options.height) && ctrl.options.height!==null) {
                    ctrl.chartOptions.chart.height = ctrl.options.height;
                    ctrl.chartHeight = '' + ctrl.options.height + 'px';
                }

                // fields filtering
                if (!angular.isUndefined(ctrl.options.fields) && ctrl.options.fields!==null) {
                    ctrl.chartRequestOptions.fields = ctrl.options.fields;
                }

                // force values format
                /*if (!angular.isUndefined(ctrl.options.format) && ctrl.options.format!==null) {
                    ctrl.defaultFormat = ctrl.options.format;
                }*/

                // force Y label
                if (!angular.isUndefined(ctrl.options.label) && ctrl.options.label!==null) {
                    ctrl.chartOptions.chart.yAxis.axisLabel = ctrl.options.label;
                    ctrl.chartOptions.chart.margin.left = 60;
                }

                // force title
                if (!angular.isUndefined(ctrl.options.title) && ctrl.options.title!==null) {
                    ctrl.chartOptions.title.enable = true;
                    ctrl.chartOptions.title.text = ctrl.options.title;
                }

                // force color
                if (!angular.isUndefined(ctrl.options.color) && ctrl.options.color!==null) {
                    if (angular.isArray(ctrl.options.color)) {
                        ctrl.chartOptions.chart.color = ctrl.options.color;
                    } else {
                        ctrl.chartOptions.chart.color = [ctrl.options.color];
                    }
                }
            }
        };

        /**
         * Finalize chart options according to directive options
         * @param data: data to parse for charting
         */
        ctrl.__finalizeChartOptions = function(data) {
            let chartData = [];
            
            switch (ctrl.options.type) {
                case 'line':
                    chartData = ctrl.__computeLineChartValues(data);
                    break;
                case 'bar':
                    chartData = ctrl.__computeBarChartValues(data);
                    break;
                case 'multibar':
                    chartData = ctrl.__computeMultiBarChartValues(data);
                    break;
                case 'pie':
                    chartData = ctrl.__computePieChartValues(data);
                    break;
            }

            // display legend only if there are some values (except for pie chart)
            if (chartData.length>1 && ctrl.options.type!=='pie') {
                ctrl.chartOptions.chart.showLegend = true;
                ctrl.chartOptions.chart.margin.top = 30;
            }   

            // set chart data and loading flag
            ctrl.chartData = chartData;
            ctrl.loading = false;
        };
        
        /**
         * Compute line chart values
         */
        ctrl.__computeLineChartValues = function(data) {
            // Output format
            //  {
            //    key: "Serie1",
            //    values: [[timestamp, value], ...]
            //  }
            const values = [];
            
            for (let name in data) {
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
        ctrl.__computeBarChartValues = function(data) {
            // Output format
            //  {
            //    key: "Serie1",
            //    bar: true,
            //    values: [[timestamp, value], ...]
            //  }
            const values = [];
            
            for (let name in data) {
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
        ctrl.__computeMultiBarChartValues = function(data) {
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
            const values = [];

            const series = [];
            for (const name in data[0]) {
                if (name!=='ts') {
                    series.push(name);
                    values.push({
                        key: name,
                        values: [],
                    });
                }
            }

            for (let value of data) {
                let index = 0;
                for (const serie of series) {
                    const lastValue = {
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
        ctrl.__computePieChartValues = function(data) {
            // Output format:
            //  [
            //    { key: "stream1", value: 0 },
            //    ...
            //  ]
            const values = [];
            
            for (const name in data) {
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
        ctrl.loadChartData = function(scope, el) {
            // set loading flag
            ctrl.loading = true;

            // prepare chart options
            ctrl.__prepareChartOptions();

            // load chart data
            if (!angular.isUndefined(ctrl.options.loadData) && ctrl.options.loadData!==null) {
                // load user data
                ctrl.options.loadData(ctrl.timestampStart, ctrl.timestampEnd)
                    .then(function(resp) {
                        ctrl.__finalizeChartOptions(resp);
                    })
                    .catch(function(error) {
                        // unable to get data, stop loading
                        console.error(error);
                        ctrl.loading = false;
                    });
            } else {
                // load device data
                const deviceUuid = ctrl.getDeviceUuid();
                chartsService.getDeviceData(deviceUuid, ctrl.timestampStart, ctrl.timestampEnd, ctrl.chartRequestOptions)
                    .then(function(resp) {
                        ctrl.__finalizeChartOptions(resp.data.data);
                    })
                    .catch(function(error) {
                        // unable to get data, stop loading
                        console.error(error);
                        ctrl.loading = false;
                    });
            }
        };

        ctrl.getDeviceUuid = function() {
            if (ctrl.options?.device?.uuid) {
                return ctrl.options.device.uuid;
            };
            return ctrl.device?.uuid;
        };

        /**
         * Change time range
         */
        ctrl.changeRange = function() {
            // compute new timestamp range
            ctrl.timestampEnd = Number(moment().format('X'));
            ctrl.timestampStart = ctrl.timestampEnd - ctrl.rangeSelector;

            // load new chart data
            ctrl.loadChartData();
        };
    }
});

