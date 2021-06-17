function () {

    var connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;

    if (connection && connection.effectiveType) return connection.effectiveType;

    return 'unknown';
}