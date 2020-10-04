            var eventStream;

            function stopEventStream() {
                if (eventStream) {
                    eventStream.close();
                }
            }

            function resetEventStream() {
                stopEventStream();
                const dataContainer = document.getElementById('data');
                dataContainer.innerHTML = '';
                const selector = document.getElementById('reportType');
                const reportType = selector.options[selector.selectedIndex].value;
                const pageNumber = document.getElementById('pageNumber').value;
                const pageSize = document.getElementById('pageSize').value;
                const sort = document.getElementById('sort').value;
                let url = `/trades/${reportType}/?`;``
                if (pageNumber && !Number.isNaN(Number.parseInt(pageNumber))) {
                    url += `page=${pageNumber}&`;
                }
                if (pageSize && !Number.isNaN(Number.parseInt(pageSize))) {
                    url += `size=${pageSize}&`;
                }
                if (sort) {
                    url += `sort=${sort}&`;
                }
                console.log(`Subscribing to ${url}`);
                eventStream = new EventSource(url, { withCredentials: true } );
                eventStream.onmessage = function(message) {
                    const contentElement = document.createElement("div");

                    const data = JSON.parse(message.data);
                    let content = `<h2>${data.snapshot ? "Snapshot" : "Update"}</h2>`;
                    content += '<ol class="list-group">';
                    data.updated.map(row => {
                        content += `<li class="list-group-item text-nowrap">${JSON.stringify(row)}</li>`;
                    });
                    data.deleted.map(row => {
                        content += `<li class="list-group-item text-nowrap list-group-item-dark">${JSON.stringify(row)}</li>`;
                    });
                    content += '</ol>';
                    contentElement.innerHTML = content;
                    dataContainer.insertBefore(contentElement, dataContainer.firstChild);
                };
            }

            console.log("Loading");
