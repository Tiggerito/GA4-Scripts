function () {

    var connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;

    if (connection && connection.saveData) return connection.saveData;

    return 'unknown';
}