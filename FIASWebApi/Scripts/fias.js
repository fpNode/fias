App = {};

App.AddrObject = (function (ko) {
    var entity = function (data) {
        var self = this;

        var mappig = {
        };

        self.FullName = function ()
        {
            var result = OFFNAME + " " + SHORTNAME;

            for (var curItem = Parent; curItem != null; curItem = curItem.Parent)
            {
                result += ", " + curItem.OFFNAME + " " + curItem.SHORTNAME;
            }
            return result;
        }

        self.Selected = ko.computed(function() {
            return App.viewModel.SelectedNode() == self;
        }, self);

        self.Select = function () {
            App.viewModel.SelectedNode(self);
        }

        ko.mapping.fromJS(ko.mapping.toJS(data), mappig, self);
    }
    return entity;
})(ko)

App.HouseObject = (function (ko) {
    var entity = function (data) {
        var self = this;

        var mappig = {
        };

        self.FullName = function () {
            var res = "";
            if (self.HOUSENUM() != "") {
                res = self.HOUSENUM();
            }
            else {
                res = "-";
            }
            if (self.BUILDNUM() != "") {
                res += ":" + self.BUILDNUM();
            }
            if (self.STRUCNUM() != "") {
                res += ":" + self.STRUCNUM();
            }
            return res;
        }

        self.Selected = ko.computed(function () {
            return App.viewModel.SelectedNode() == self;
        }, self);

        self.Select = function () {
            App.viewModel.SelectedNode(self);
        }

        ko.mapping.fromJS(ko.mapping.toJS(data), mappig, self);
    }
    return entity;
})(ko)

App.viewModel = (function (ko) {
    var model = {};

    model.status = ko.observable(null);
    model.errorMsg = ko.observable(null);

    model.CurrentAddr = ko.observable(null);
    model.SelectedNode = ko.observable(null);

    model.Search = function ()
    {
        model.CurrentAddr(null);
        model.SelectedNode(null);
        model.HouseObjects.removeAll();
        model.status('search');

        $.ajax({
            url: apiUrl,
            type: 'get',
            data: { query: $('#fias-search').val()},
            dataType: 'text',
            cache: false
        }).done(function (data) {
            model.status('ok');
            model.errorMsg("");

            model.AddrObjects(ko.utils.arrayMap(JSON.parse(data), function (item) {
                return new App.AddrObject(item);
            }));

        }).fail(function (jqXHR, textStatus) {
            model.status('error');
        });
    }

    model.SearchHouse = function (addr) {
        model.CurrentAddr(addr);
        model.SelectedNode(addr);

        model.AddrObjects.removeAll();

        model.status('search');

        $.ajax({
            url: apiUrl2,
            type: 'get',
            data: { AddrId: addr.AOGUID() },
            dataType: 'text',
            cache: false
        }).done(function (data) {
            model.status('ok');
            model.errorMsg("");

            model.HouseObjects(ko.utils.arrayMap(JSON.parse(data), function (item) {
                return new App.HouseObject(item);
            }));

        }).fail(function (jqXHR, textStatus) {
            model.status('error');
        });
    }

    model.AddrObjects = ko.observableArray();
    model.HouseObjects = ko.observableArray();


    model.Save = function () {
    }

    return model;
})(ko);

$(document).ready(function () {
    // Initiate the Knockout bindings
    ko.applyBindings(App.viewModel, document.getElementById('viewModel'));
})

