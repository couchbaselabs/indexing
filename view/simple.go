package view

const templStart = 
`
function (doc, meta) {
  if (meta.type != "json") return;`

const templFunctions =
`
  var stringToUtf8Bytes = function (str) {
    var utf8 = unescape(encodeURIComponent(str));
    var bytes = [];
    for (var i = 0; i < str.length; ++i) {
        bytes.push(str.charCodeAt(i));
    }
    return bytes;
  };

  var indexFormattedValue = function (val) {
    if (val === null) {
      return [64];
    } else if (typeof val == "boolean") {
      return [96, val];
    } else if (typeof val == "number") {
      return [128, val];
    } else if (typeof val == "string") {
      return [160, stringToUtf8Bytes(val)];
    } else if (typeof val == "object") {
      if (val instanceof Array) {
        return [192, val];
      } else {
        return [224, val];
      }
    }
  };`

const templExpr =
`
  var $var = indexFormattedValue($path);`

const templKey =
`
  var key = [$keylist];`

const templEmit =
`
  emit(key, null);`

const templEnd =
`
}
`
