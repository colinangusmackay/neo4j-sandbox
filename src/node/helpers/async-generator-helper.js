"use strict";

(function(){
    var run = function(generator){
        var sequence;
        var process = function(result){
            result.value.then(function(value){
                if (!result.done){
                    process(sequence.next(value))
                }
            }, function (error){
                if (!result.done){
                    process(sequence.throw(error));
                }
            })
        }
        sequence = generator();
        var next = sequence.next();
        process(next);
    }
    module.exports.run = run;
}());