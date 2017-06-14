$(function() {
    renderJobsFullView();
    renderTasksFullView();
    bindSandboxButtons();
    renderBreadCrumbMenu();
});

function renderJobsFullView() {
    var jobName = $("#index-job-name").text();
    $("#jobs-full-view-tbl").bootstrapTable({
         url: "/api/operate/jobFullView/" + jobName,
         columns:
         [{
             field: "jobName",
             title: "作业名称"
         }, {
             field: "times",
             title: "待运行次数"
         }]
    });
}

function renderTasksFullView() {
    var jobName = $("#index-job-name").text();
    $("#tasks-full-view-tbl").bootstrapTable({
         url: "/api/operate/taskFullView/" + jobName,
         columns:
         [{
             field: "taskId",
             title: "任务ID"
         }, {
             field: "serverIp",
             title: "主机"
         }, {
             field: "taskStatus",
             title: "状态",
             formatter: "statusFormatter"
         }, {
             field: "sandbox",
             title: "sandbox",
             formatter: "sandboxFormatter"
         }]
    });
}

function statusFormatter(value) {
    if ("RUNNING" !== value && "FAILOVER" !== value) {
        return "<p style='color:red'>" + value + "</p>"
    } else {
        return value;
    }
}

function sandboxFormatter(value) {
    if ("" !== value) {
        return "<button operation='sandbox' class='btn-xs btn-info' sandbox='" + value + "'>sandbox</button>"
    } else {
        return "-";
    }
}

function bindSandboxButtons(value) {
    $(document).off("click", "button[operation='sandbox']");
    $(document).on("click", "button[operation='sandbox']", function(event) {
        var sandbox = $(event.currentTarget).attr("sandbox");
        window.open("http://" + sandbox);
    });
}

function renderBreadCrumbMenu() {
    $("#breadcrumb-job").click(function() {
        $("#content-right").load("html/job/jobs_overview.html");
    });
}
