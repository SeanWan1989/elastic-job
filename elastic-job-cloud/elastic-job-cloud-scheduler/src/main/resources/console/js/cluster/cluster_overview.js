$(function() {
    $(".toolbar input").bind("keypress", function(event) {
        if("13" == event.keyCode) {
            $("#mesos-slave-table").bootstrapTable("refresh", {silent: true});
        }
    });
});

function queryParams(params) {
    return {
        roleName: $.trim($("#role-name").val()),
    };
}

function hostnameFormatter(val, row) {
    return val;
}

function cpuFormatter(val, row) {
    return row.use_resources.cpus + " / " + row.offered_resources.cpus + " / " + row.resources.cpus;
}

function cpuRolesFormatter(val, row) {
    var result = "";
    val.forEach(function(each) {
        if(each.cpus == undefined) {
            return;
        }
        var cpuInfo = getResourceValue(each.cpus.used_resources) + " / " + getResourceValue(each.cpus.offered_resources) + " / " + getResourceValue(each.cpus.reserved_resources);
        result += cpuInfo + "(" + each.role_name + ")</br>";
    });
    return result;
}

function memoryFormatter(val, row) {
    return formatMemory(row.use_resources.mem) + " / " + formatMemory(row.offered_resources.mem) + " / " + formatMemory(row.resources.mem);
}

function memoryRolesFormatter(val, row) {
    var result = "";
    val.forEach(function(each) {
        if(each.mem == undefined) {
            return;
        }
        var memInfo = formatMemory(getResourceValue(each.mem.used_resources)) + " / " + 
                      formatMemory(getResourceValue(each.mem.offered_resources)) + " / " + 
                      formatMemory(getResourceValue(each.mem.reserved_resources));
        result += memInfo + "(" + each.role_name + ")</br>";
    });
    return result;
}

function getResourceValue(resource) {
    var result = "0";
    if(resource == undefined) {
        return result;
    }
    if(resource.type == 'SCALAR') {
        result = resource.value;
    } else if(resource.type == 'RANGES') {
        result = resource.begin + "-" + resource.end;
    }
    return result;
}

function formatMemory(mb) {
    if(mb == null || mb == 0) {
        return "0B";
    } else if(mb < 1024) {
        return mb + "MB";
    } else {
        return (mb / 1024).toFixed(1) + "GB";
    }
}

function twoRegisteredTimeFormatter(val, row) {
    return formatUnixtime(row.registered_time) + " / </br>" + formatUnixtime(row.reregistered_time);
}

function formatUnixtime(val) {
    if(val == undefined || val == "") {
        return "-";
    }
    return new Date(val * 1000).format("yyyy-MM-dd HH:mm:ss");
}
