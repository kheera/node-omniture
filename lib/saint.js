var util = require("util"),
		Client = require("./client");
		
util.inherits(Saint, Client);
		
function Saint(username, sharedSecret, environment, options){
	this.defaultWaitTime = 5;
	this.waitTime = (options && options.waitTime) ? options.waitTime : this.defaultWaitTime;
	this.methods = {
		exportCreateJob: 'Saint.ExportCreateJob',
		exportGetStatus: 'Saint.CheckJobStatus',
		exportGetFileSegment: 'Saint.ExportGetFileSegment',
		importCreateJob: 'Classifications.CreateImport',
		importPopulateJob: 'Classifications.PopulateImport',
		importCommitJob: 'Classifications.CommitImport'
	}

	Client.apply(this, arguments);
}
var p = Saint.prototype;
p.clientRequest = Client.prototype.request;
p.getJob = function(parameters, callback){
	var self = this;
	this.clientRequest(this.methods.exportCreateJob, parameters, function(err, data){
		if(err){ callback(new Error(err.message)); }
		if(data.errors){
			callback(new Error(data.errors[0]));
		}else{
			self.getQueuedJob(data, callback);
		}
	});
}

p.getQueuedJob = function(jobId, callback){
	this.logger("info", "Getting Queued Job");
	var self = this,
			jobData = {"job_id" : jobId };
	this.clientRequest(this.methods.exportGetStatus, jobData, function(err, data){
		if(err){ 
			callback(err, data); 
		}else{
			var json = JSON.parse(data);

			if(json[0].status.toLowerCase() == 'completed' && json[1] && json[1].status.toLowerCase() == 'ready'){
				if(parseInt(json[1].viewable_pages,10) > 0){
					callback(null, json);
				}else{
					callback(new Error("There were no viable pages for SAINT job "+jobId));
				}
			}else if(json.status == "failed"){
				callback(new Error(json.status+": "+json.error_msg));
			}else{
				self.logger("info", "Job: "+jobId+" not ready yet");
				setTimeout(function(){
					self.logger("info", "Checking job: "+jobId+" status");
					self.getQueuedJob(jobId, callback);
				}, self.waitTime * 1000)
			}
		}
	});
};

p.getJobSegment = function(fileId, pageNum, callback){
	var self = this,
			segmentData = {"file_id": fileId, "segment_id": pageNum }
	this.clientRequest(this.methods.exportGetFileSegment, segmentData, function(err, data){
		if(err){ callback(new Error(err.message)); };
		var json = JSON.parse(data);
		if(json.length > 0){
			json = json[0];
			if(json.header == null || json.data == null){
				callback(new Error('No data was returned: ' + data));
			}else{
				callback(null, json);
			}
		}else{
			callback(new Error('Malformed formatted data response: '+ data));
		}
	});
}

p.importJob = function(jobDescription, jobData, callback){
	var self = this,
			populateData =[];
	while(jobData.length){
		populateData.push(jobData.splice(0,10000));
	}

	self.logger("info", "Creating Job");
	console.log('jobDescription: ' + jobDescription);
	this.importCreateJob(jobDescription, myFunction);

	function myFunction(err, data){
		console.log('Adobe create job results(err): ' + err);
		console.log('Adobe create job results(data): ' + data);
		if(err){ callback(new Error(err.message)); return; };
		var jobId = data;
		self.logger("Populating Job "+jobId);
		populateData.forEach(function(item, index){
			var reportData = {
				job_id: jobId,
				page: (index + 1),
				rows: item
			};
			console.log('importing ', reportData);
			self.importPopulateJob(reportData, function(err, data){
				self.logger("info", "Populated page "+ index);
				if(err){
					callback(new Error(err.message));
				}else{
					if(index == populateData.length - 1){
						self.logger("info", "Committing job " + jobId);
						self.importCommitJob(jobId, function(err, data){
							if(err){ callback(new Error(err.message)); }
							callback(null, data);
						});
					}
				}
			});
		});
	}
}

p.importCreateJob = function(jobDescription, callback){
	var self = this;
	this.request(this.methods.importCreateJob, jobDescription, doesSomething);
	function doesSomething(err, data){
		console.log("Adobe real response: " + data);
		var json = JSON.parse(data);
		if(err || data.toLowerCase() == 'failed' || typeof json.errors != 'undefined'){
			var message = '';
			if(err && err.message){
				message = err.message;
			}else if (json.errors.length){
				message = json.errors[0];
			}else{
				message = "failed to create import job";
			}
			callback(new Error(message));
		}else{
			callback(null, json.job_id);
		}
	}
}

p.importPopulateJob = function(populateData, callback){
	var self = this;
	this.request(this.methods.importPopulateJob, populateData, function(err, data){
		var json = JSON.parse(data);

		if(err || data.toLowerCase() == 'failed' || typeof json.errors != 'undefined'){
			var message = '';
			if(err && err.message){
				message = err.message;
			}else if (json.errors.length){
				message = json.errors[0];
			}else{
				message = "Failed to populate job " + jobId	
			}
			callback(new Error(message)); 
		}else{
			callback(null, data);
		}
	});
}

p.importCommitJob = function(jobId, callback){
	var self = this;
	this.request(this.methods.importCommitJob, {job_id: jobId}, function(err, data){
		var json = JSON.parse(data);
		if(err || data.toLowerCase() == 'failed' || typeof json.errors != 'undefined'){ 
			var message = '';
			if(err && err.message){
				message = err.message;
			}else if(json.errors){
				message = json.errors[0];
			}else{
				message = "SAINT JOB: "+jobId+" failed to import";
			}
			callback(new Error(message)); 
		}else{
			callback(null, jobId);
		}
	});
}

module.exports = Saint;
