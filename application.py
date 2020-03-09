import json
import datetime
import http.server
import cgi

class InputProcessor():
    NAMES = "user tenant method url subscription timestamp x-api-key payloadSize".split()
    indexof_ts = NAMES.index("timestamp")
    indexof_subscr = NAMES.index("subscription")
    indexof_tenant = NAMES.index("tenant")
    indexof_user = NAMES.index("user")
    indexof_size = NAMES.index("payloadSize")
    indexof_method = NAMES.index("method")


    def __init__(self, input_json):
        self.raw_data = json.loads(input_json)
        assert isinstance(self.raw_data, list)
        for entry in self.raw_data:
            assert isinstance(entry, dict)
            assert set(entry.keys()) == set(self.NAMES)
        # Ensure sorting and check types.
        self.data = [[r[k] for k in self.NAMES[:5]] +
                     [datetime.datetime.strptime(r[self.NAMES[5]], "%d.%m.%Y %H:%M")] +
                     [int(r[k]) for k in self.NAMES[-2:]]
                     for r in self.raw_data]

    def get_columns(self):
        return self.NAMES

    def get_events_for_month(self, year, month, subscription):
        return [r for r in self.data if r[self.indexof_ts].year == year and
                                        r[self.indexof_ts].month == month and
                                        r[self.indexof_subscr] == subscription]

    def events_interval(self):
        tss = [r[self.indexof_ts] for r in self.data]
        return (min(tss), max(tss))

    def get_subscriptions_dict(self):
        return dict([(r[self.indexof_subscr], r[self.indexof_tenant]) for r in self.data])

class Model():
    def __init__(self, parsed_input):
        assert isinstance(parsed_input, InputProcessor)
        self.input = parsed_input
        self.calculate_charging_units()
        self.charge_tenants()

    def iterate_months(self):
        def next_month(y, m):
            if m == 12:
                return (y + 1, 1)
            return (y, m + 1)
        (start, stop) = self.input.events_interval()
        it = (start.year, start.month)
        end = (stop.year, stop.month)
        while True:
            yield it
            if it == end:
                return;
            it = next_month(*it)

    def calculate_charging_units(self):
        """
        Returns number of pricing events by tenant (company) and month.
        """
        self.charging_units_table = {}
        self.subscriptions = self.input.get_subscriptions_dict()
        for subscription in self.subscriptions:
            tenant = self.subscriptions[subscription]
            self.charging_units_table[(tenant, )] = []
            self.charging_units_table[subscription] = []

        index = 0
        for (y, m) in self.iterate_months():
            for k in self.charging_units_table:
                self.charging_units_table[k].append(self.get_null_count())
            for subscription in self.subscriptions:
                events = self.input.get_events_for_month(y, m, subscription)
                tenant = self.subscriptions[subscription]
                indexofuser = 0;
                charging_units = self.get_charging_units(events)
                self.charging_units_table[(tenant, )][-1] = self._add(
                        self.charging_units_table[(tenant, )][-1], charging_units)
                self.charging_units_table[subscription][-1] = charging_units

    def charge_tenants(self):
        tenants = self.subscriptions.values()
        self.charging_table = {}
        for tenant in tenants:
            self.charging_table[tenant] = []
            for item in self.charging_units_table[(tenant,)]:
                self.charging_table[tenant].append(self.assign_price(item))

    @staticmethod
    def _add(t1, t2):
        return tuple(map(sum, zip(t1, t2)))

    def print_model_results(self):
        rv = ""
        for tenant in sorted(set(self.subscriptions.values())):
            rv += "Tenant: %s\n" % tenant
            rv += "=======================\n\n"
            rv += "Month   Calculated consumption  Breakdown per subscription (units)\n"
            rv += "        ($)                     "
            tenant_subsriptions = [s for s in self.subscriptions \
                                   if self.subscriptions[s] == tenant]
            rv += "".join(map(lambda s: "%-17s" % s, tenant_subsriptions)) + "\n"
            rv += "-" * (30 + len(tenant_subsriptions * 17)) + "\n"

            index = 0
            for (y, m) in self.iterate_months():
                rv += "%2d/%4d   " % (m, y)
                rv += "%20.0f" % self.charging_table[tenant][index]
                rv += "  "
                for s in tenant_subsriptions:
                    rv += "%-17s" % (self.charging_units_table[s][index], )

                rv += "\n"
                index += 1

            rv += "Total:    %20.0f" % sum(self.charging_table[tenant])
            rv += "\n\n"
        return rv

    def json_model_results(self):
        rv = "[ "
        tenant_entries = []
        for tenant in sorted(set(self.subscriptions.values())):
            tenant_subsriptions = [s for s in self.subscriptions \
                                   if self.subscriptions[s] == tenant]
            tenant_entry = "{ "
            tenant_entry += "\"tenant\": \"%s\", " % tenant
            tenant_entry += "\"calculated_consumptions\": [ "
            index = 0

            tenant_monthly = []
            for (y, m) in self.iterate_months():
                item = "{ \"month\": \"%2d/%4d\", \"consumption\": %f, \"breakdown\": [ " % (m, y, self.charging_table[tenant][index])
                breakdown_items = []
                for s in tenant_subsriptions:
                    breakdown_item = "{ \"subscription\": \"%s\"," % s
                    breakdown_item += "\"consumed\": \"%s\" }" % (self.charging_units_table[s][index], )
                    breakdown_items.append(breakdown_item)
                item += ", ".join(breakdown_items)
                item += " ] }"
                tenant_monthly.append(item)
            tenant_entry += ", ".join(tenant_monthly)
            tenant_entry += " ] }"
            tenant_entries.append(tenant_entry)
        rv = "[ " + ", ".join(tenant_entries) + " ]"
        return rv

class Model1(Model):
    """
    Model 1 is per active user pricing.
    Price is determined by number of active users in given month.
    The pricing is then as follow:

    Active users Price/unit
     0-5          2000
     6-20         1500
     21-50        1000
     51-100       500
     101+         fixed price 10000

    """

    @classmethod
    def get_null_count(cls):
        return (0,);

    @classmethod
    def get_charging_units(cls, events):
        return (len(set([e[InputProcessor.indexof_user] for e in events])),)

    @classmethod
    def assign_price(cls, charging_units):
        pricing_table = [(5, 2000), (15, 1500), (30, 1000), (50, 500), (1, 10000)]
        (charging_units, ) = charging_units
        price = 0
        for (threshold, price_per_unit) in pricing_table:
            considered_events = min(threshold, charging_units)
            charging_units -= considered_events
            price += considered_events * price_per_unit
            if charging_units == 0:
                return price
        return price

class Model2(Model):
    """
    Model 2 charges per GET events.
    Users are charged by getting stored data. Saving data is free.
    Price is determined by charging units, where each GET operation of certain
    size generates the same amount of charging units.
    The pricing is then as follow:

    Units      Price/unit
     0-5        2000
     6-20       1500
     21-50      1000
     51-100     500
     101+       fixed price 10000 

    """

    @classmethod
    def get_null_count(cls):
        return (0,);

    @classmethod
    def get_charging_units(cls, events):
        get_sizes = [e[InputProcessor.indexof_size] for e in events if e[InputProcessor.indexof_method] == "GET"]
        return (sum(get_sizes),)

    @classmethod
    def assign_price(cls, charging_units):
        pricing_table = [(5, 2000), (15, 1500), (30, 1000), (50, 500), (1, 10000)]
        (charging_units, ) = charging_units
        price = 0
        for (threshold, price_per_unit) in pricing_table:
            considered_events = min(threshold, charging_units)
            charging_units -= considered_events
            price += considered_events * price_per_unit
            if charging_units == 0:
                return price
        return price

class Model3(Model):
    """
    Model 3 charges per POST and PUT events.
    Users are charged by storing/modifying data. Retrieving data is free.
    Price is determined by storage units. Storing 1 unit of data with POST operation
    cost 1 storage unit. Changing 1 unit of data with PUT operation costs 1,5 unit.
    The pricing is then as follow:

    Units      Price/unit
     0-5        2000
     6-20       1500
     21-50      1000
     51-100     500
     101+       fixed price 10000

    """

    @classmethod
    def get_null_count(cls):
        return (0, 0);

    @classmethod
    def get_charging_units(cls, events):
        post_sizes = [e[InputProcessor.indexof_size] for e in events if e[InputProcessor.indexof_method] == "POST"]
        put_sizes = [e[InputProcessor.indexof_size] for e in events if e[InputProcessor.indexof_method] == "PUT"]
        return (sum(post_sizes), sum(put_sizes))

    @classmethod
    def assign_price(cls, charging_units):
        pricing_table = [(5, 2000), (15, 1500), (30, 1000), (50, 500), (1, 10000)]
        (post_units, put_units) = charging_units
        charging_units = post_units + put_units * 1.5
        price = 0
        for (threshold, price_per_unit) in pricing_table:
            considered_events = min(threshold, charging_units)
            charging_units -= considered_events
            price += considered_events * price_per_unit
            if charging_units == 0:
                return price
        return price

def process_task(body):
    try:
        data = InputProcessor(body)
    except TypeError:
        return "Error parsing input file!"
    rv = ""
    rv += "\n*** Model 1 ***\n***************\n\n"
    rv += Model1(data).print_model_results()
    rv += "\n*** Model 2 ***\n***************\n\n"
    rv += Model2(data).print_model_results()
    rv += "\n*** Model 3 ***\n***************\n\n"
    rv += Model3(data).print_model_results()
    return rv

def process_task_json(body):
    try:
        data = InputProcessor(body)
    except:
        return "\"Error parsing input file!\""
    rv = ""
    rv += "{ \"results\": [\n"
    rv += "{ \"model\": \"Model 1\", \"result\":"
    rv += Model1(data).json_model_results()
    rv += "},\n"

    rv += "{ \"model\": \"Model 2\", \"result\":"
    rv += Model2(data).json_model_results()
    rv += "},\n"

    rv += "{ \"model\": \"Model 3\", \"result\":"
    rv += Model3(data).json_model_results()
    rv += "}\n"

    rv += "] }"
    return rv

class ChargingHTTPRequestHandler(http.server.BaseHTTPRequestHandler):
    welcome_message = b"""
<html>
<head><title> Charging service. </title></head>
<body>
<p> Insert input json data: </p>
<form action="/" enctype="multipart/form-data" method="post">
<label for="file">Select a file:</label>
<input type="file" id="file" name="file">
<input type="submit" value="Submit">
</form>
</body>
</html>
"""

    response_message = b"""
<html>
<head><title> Charging service. </title></head>
<body>
<pre>
%s

</pre>
</body>
</html>
"""

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(self.welcome_message)

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        self.send_response(200)
        self.end_headers()
        ctype, pdict = cgi.parse_header(self.headers['content-type'])
        if ctype == 'multipart/form-data':
            pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
            fields = cgi.parse_multipart(self.rfile, pdict)
            assert len(fields) == 1
            body = fields.get('file')[0]
            response = process_task(body)
            if isinstance(response, str):
                response = response.encode()
            assert isinstance(response, bytes)
            response = self.response_message % response
        elif ctype == "application/json":
            body = self.rfile.read(content_length)
            response = process_task_json(body)
            if isinstance(response, str):
                response = response.encode()
            assert isinstance(response, bytes)
        else:
            self.wfile.write("Content format not understood")
            return

        self.wfile.write(response)

if __name__ == '__main__':
    import os
    assert 'CHARGING_APP_PORT' in os.environ, "Please specify CHARGING_APP_PORT variable"
    listen_port = os.environ['CHARGING_APP_PORT']
    assert listen_port.isdigit()
    listen_port = int(listen_port)
    assert listen_port >= 1000 and listen_port < 65536
    server_address = ('', listen_port)
    httpd = http.server.HTTPServer(server_address, ChargingHTTPRequestHandler)
    httpd.serve_forever()
