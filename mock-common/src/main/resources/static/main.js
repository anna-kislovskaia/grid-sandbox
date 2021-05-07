            var eventStream;

            function stopEventStream() {
                if (eventStream) {
                    eventStream.close();
                    eventStream = undefined;
                }
                const subscriptionId = document.getElementById('subscriptionId');
                subscriptionId.innerText='';
                // cleanup user filters
                document.getElementById('filters').innerText = '';
                document.getElementById('userFilter').value = '';
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
                    if (data.snapshot) {
                        const subscriptionId = document.getElementById('subscriptionId');
                        subscriptionId.innerText = data.subscriptionId;
                    }
                    const filters = document.getElementById('filters');
                    filters.innerText = JSON.stringify(data.filterOptions);

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

            function updateSubscriptionParams() {
                const formData  = new FormData();
                const subscriptionId = document.getElementById('subscriptionId').innerText;
                const page = document.getElementById('pageNumber').value;
                const size = document.getElementById('pageSize').value;
                const sort = document.getElementById('sort').value;
                const filter = document.getElementById('userFilter').value;

                const params = {subscriptionId, page, size, sort, filter};
                for(const name in params) {
                    formData.append(name, params[name]);
                }
                console.log(`Update subscription parameters to ${JSON.stringify(params)}`);
                fetch('/trades/params', {
                    method: 'PUT',
                    body: formData,
                    credentials: 'same-origin'
                })
            }

            console.log("Loading");
