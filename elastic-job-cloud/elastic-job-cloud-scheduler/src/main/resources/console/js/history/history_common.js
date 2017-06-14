$(function() {
    $(".custom-datepicker").daterangepicker({singleDatePicker : true, timePicker : true, timePicker24Hour : true, timePickerSeconds : true, autoUpdateInput : false});
    $(".custom-datepicker").on("apply.daterangepicker", function(event, picker) {
        $(this).val(picker.startDate.format("YYYY-MM-DD HH:mm:ss"));
    });
    $(".custom-datepicker").on("cancel.daterangepicker", function(event, picker) {
        $(this).val("");
    });
});

function dateTimeFormatter(value) {
    if(null == value){
        return "";
    }
    return new Date(value).format("yyyy-MM-dd HH:mm:ss");
}

function showHistoryMessage(value) {
    $("#history-message").html(value);
    $("#history-message-modal").modal("show");
}
