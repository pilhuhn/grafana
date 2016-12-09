package hawkular


import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"

	//"golang.org/x/net/context/ctxhttp"

	"github.com/grafana/grafana/pkg/log"
	"github.com/grafana/grafana/pkg/models"
	//"github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/tsdb"
	"strings"
	"golang.org/x/net/context/ctxhttp"
	"bytes"
	"github.com/grafana/grafana/pkg/components/simplejson"
	"io/ioutil"
	"gopkg.in/guregu/null.v3"
)

type HawkularDBExecutor struct {
	*models.DataSource
	httpClient *http.Client
	log log.Logger
}

func NewHawkularDBExecutor(datasource *models.DataSource) (tsdb.Executor, error) {
	return &HawkularDBExecutor{
		DataSource: datasource,
		log:        log.New("tsdb.hawkular"),
	}, nil
}

func init() {
	log.Info("HawkularDBE.init")
	// TODO the datasource name should only by 'hawkular, but the
	// plugin had this to be named hawkular-datasource (iirc)
	tsdb.RegisterExecutor("hawkular-datasource", NewHawkularDBExecutor)
}

func (e *HawkularDBExecutor) Execute(ctx context.Context, queries tsdb.QuerySlice, context *tsdb.QueryContext) *tsdb.BatchResult {

	log.Info("HawkularDBE.execute")
	result := &tsdb.BatchResult{}


	fromTime := context.TimeRange.GetFromAsMsEpoch()
	endTime  := context.TimeRange.GetToAsMsEpoch()

	for _, query := range queries {

		var tsdbQuery HawkularTsdbQuery

		tsdbQuery.Start = fromTime
		tsdbQuery.End = endTime
		tsdbQuery.Order = "ASC"

		isTags := query.Model.Get("queryBy").MustString() == "tags"

		if isTags {
			tsdbQuery.Tags, _ = fetchTags(query)
		} else {
			tsdbQuery.Ids = fetchIds(query)
		}

		isTags, metric := e.buildMetric(query)


		//if setting.Env == setting.DEV {
		log.Info("HawkularTsdb request", "params", tsdbQuery)
		log.Info("HawkularTsdb metric", "metric", metric)
		//}

		req, err := e.createRequest(tsdbQuery, query.Model.Get("type").MustString())
		if err != nil {
			result.Error = err
			return result
		}

		res, err := ctxhttp.Do(ctx, e.httpClient, req)
		if err != nil {
			result.Error = err
			return result
		}

		queryResult, err := e.parseResponse(tsdbQuery, res)
		if err != nil {
			return result.WithError(err)
		}

		result.QueryResults = queryResult
	}
	return result

}
func fetchIds(query *tsdb.Query) []string {
	var result []string

	target := query.Model.Get("target").MustString()

	result = append(result, target)

	return result
}

func fetchTags(query *tsdb.Query) (string, error) {
	var result string = ""
	model := query.Model

	for _,t := range model.Get("tags").MustArray() {
		tagJson := simplejson.NewFromAny(t)
		tag := &Tag{}
		var err error

		tag.Name, err = tagJson.Get("name").String()
		if err != nil {
			return "", err
		}

		tag.Value, err = tagJson.Get("value").String()
		if err != nil {
			return "", err
		}

		aTag := fmt.Sprintf("%s:%s", tag.Name, tag.Value)

		if result != "" {
			result = fmt.Sprintf("%s,%s", result, aTag)
		} else {
			result = aTag
		}
	}

	return result, nil
}

func (e *HawkularDBExecutor) createRequest(data HawkularTsdbQuery, qType string) (*http.Request,
error) {

	u, _ := url.Parse(e.Url)
	u.Path = path.Join(u.Path, fmt.Sprintf("/%ss/raw/query", qType )) // TODO %ss -> real func

	postData, err := json.Marshal(data)

	req, err := http.NewRequest(http.MethodPost, u.String(), strings.NewReader(string(postData)))
	if err != nil {
		log.Info("Failed to create request", "error", err)
		return nil, fmt.Errorf("Failed to create request. error: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Hawkular-Tenant", e.DataSource.JsonData.Get("tenant").MustString())
	if e.BasicAuth {
		req.SetBasicAuth(e.BasicAuthUser, e.BasicAuthPassword)
	}

	return req, err

}

func (e *HawkularDBExecutor) parseResponse(query HawkularTsdbQuery, res *http.Response) (map[string]*tsdb.QueryResult,
error) {

	queryResults := make(map[string]*tsdb.QueryResult)
	queryRes := tsdb.NewQueryResult()

	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return nil, err
	}

	if res.StatusCode/100 != 2 {
		log.Info("Request failed", "status", res.Status, "body", string(body))
		return nil, fmt.Errorf("Request failed status: %v", res.Status)
	}

	var data []Series
	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Info("Failed to unmarshal hawkular response", "error", err, "status", res.Status, "body", string(body))
		return nil, err
	}


	for _, val := range data {
		log.Info(val.Id)

		series := tsdb.TimeSeries{
			Name: val.Id,
		}

		for _, point := range val.Points {
			timestamp := point.Time
			if err != nil {
				log.Info("Failed to unmarshal opentsdb timestamp", "timestamp", timestamp)
				return nil, err
			}
			series.Points = append(series.Points, tsdb.NewTimePoint(null.FloatFrom(point.Value), timestamp))
		}

		queryRes.Series = append(queryRes.Series, &series)
	}

	queryResults["A"] = queryRes
	return queryResults, nil

	return nil, nil
}

func (e *HawkularDBExecutor) buildMetric(query *tsdb.Query) (bool, map[string]interface{} ) {

	/* With tags:
	INFO[12-09|12:13:35] {
	  "queryBy": "tags",
	  "rate": false,
	  "refId": "A",
	  "seriesAggFn": "none",
	  "tags": [
	    {
	      "name": "heap",
	      "value": "used"
	    }
	  ],
	  "target": "select metric",
	  "timeAggFn": "avg",
	  "type": "gauge"
	}
	 */

	/* With single id
	{
	  "queryBy": "ids",
	  "rate": false,
	  "refId": "A",
	  "seriesAggFn": "none",
	  "tags": [],
	  "target": "MI~R~[d9c17e5f-c9b4-4d22-af2d-b9a40831c3af/Local~~]~MT~WildFly Memory Metrics~Heap Used",
	  "timeAggFn": "avg",
	  "type": "gauge"
	}
	 */



	metric := make(map[string]interface{})

	b, err := query.Model.EncodePretty()
	if (err != nil) {
		log.Info("error %s", err.Error() )
	}
	tmpString := bytes.NewBuffer(b).String()
	log.Info(tmpString)

	// Setting metric and aggregator
	metric["metric"] = query.Model.Get("measurement").MustString()
	metric["timeAggFn"] = query.Model.Get("timeAggFn").MustString()
	metric["name"] = query.Model.Get("alert.name").MustString()
	metric["query"] = query.Model.Get("query").MustString()
	metric["rawQuery"] = query.Model.Get("rawQuery").MustString()
	metric["interval"] = query.Model.Get("interval").MustString()
	metric["key"] = query.Model.Get("key").MustString()
	metric["value"] = query.Model.Get("value").MustString()
	metric["operator"] = query.Model.Get("operator").MustString()

	// Setting tags
	tags, tagsCheck := query.Model.CheckGet("tags")
	if tagsCheck && len(tags.MustMap()) > 0 {
		metric["tags"] = tags.MustMap()
	}

	isTags := query.Model.Get("queryBy").MustString() == "tags"

	return isTags, metric

}