package main

import (
	"bufio"
	"fmt"
	"github.com/golang/protobuf/proto"

	//"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"io"
	//	"io/ioutil"
	"log"
	"math/rand"
	pb "myrtreego"
	rg "myrtreego/rtreego"
	"os"
	"strconv"
	"strings"
	//	"time"
	"github.com/garyburd/redigo/redis"
)




func main(){
   //dial redis
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}
	defer c.Close()



	f, err := os.Open("G:\\go_projects\\bank320211.csv")
	if err != nil {
	//	return nil, err
	   fmt.Println("read csv error,err",err)
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	 var i=0
     var things []rg.Spatial
	for {

		gbk_line, err := rd.ReadString('\n') //以'\n'为结束符读入一行
		if err != nil || io.EOF == err {
			break
		}
		i++
		split_line := strings.Split(gbk_line, ",")
	// 	fmt.Println("...",split_line[18])//地点名称
	//	fmt.Println("...",gbk_line[strings.Index(gbk_line, "["):len(gbk_line)-3])//经纬度

		coor:=gbk_line[strings.Index(gbk_line, "["):len(gbk_line)-3]
		newcoor:=coor[1:len(coor)-1]
 		newcoorArr := strings.Split(newcoor, ",")
		lon,err := strconv.ParseFloat(newcoorArr[0],64)
		lat,err := strconv.ParseFloat(newcoorArr[1],64)
		 p1 := rg.Point{lon,lat}
	//	fmt.Println("newcoor",lon,lat)
	    things = append(things, &rg.Thing{p1, split_line[18]})
	}

	//rt := rg.NewTree(2, 25, 50,things...)

	for i := 0; i < 1000; i++ {
		p1 := rg.Point{getNJLgt(),getNJLat()}
		//	fmt.Println("newcoor",lon,lat)
		things = append(things, &rg.Thing{p1, "virtual "+string(i)})
	}

    var total int=len(things)
 	var cellmap = make(map[string][]rg.Spatial)
	 var codeStr string
 	fmt.Println("len cellmap",len(cellmap))
	fmt.Println("len things", total)
	for _, elem := range things {
		codeStr,_=rg.Encode(elem.(*rg.Thing).Location[0],elem.(*rg.Thing).Location[1],10)
    //    fmt.Println(codeStr,".....",elem.(*rg.Thing).Location[0],".....",elem.(*rg.Thing).Location[1],".....",elem.(*rg.Thing).Name)
		cellmap[codeStr[0 : 5]]=append(cellmap[codeStr[0 : 5]], elem)
	}
	fmt.Println("len cellmap",len(cellmap))
	for key, val := range cellmap{
		fmt.Println("Key:", key,"...",len(val) )

		rt := &rg.Rtree{
			Dim:         2,
			MinChildren: 25,
			MaxChildren: 50,
		}
		td:=rt.BulkLoad(val)
		proStu, err := proto.Marshal(td)
		_, err = c.Do("SET", key, proStu)
		if err != nil {
			fmt.Println("redis set failed:", err)
		}
		fmt.Println("size..",rt.MYSize())
		pp:= rg.Point{120.231038,31.562171}
		bb := &rg.Thing{pp, "y"}

		// Get a slice of the objects in rt that intersect bb:

		results := rt.SearchIntersect(bb.Bounds(),rg.LimitFilter(10))
		fmt.Println("results....",len(results))

		for _, v := range results {
			//	fmt.Println(v.(*Thing).name) //prints a, b, c
			fmt.Println(v) //prints a, b, c
		}
		fmt.Println(".........") //prints a, b, c

		// Get a slice of the k objects in rt closest to q:
		results,dists := rt.NearestNeighbors(5,pp)
		for _, v := range results {
			//	fmt.Println(v.(*Thing).name) //prints a, b, c
			fmt.Println(v) //prints a, b, c
		}
		for _, v := range dists {
			//	fmt.Println(v.(*Thing).name) //prints a, b, c
			fmt.Println(v) //prints a, b, c
		}

		fmt.Println("reload...");
		var mytd pb.TreeData
		  in,_:=redis.Bytes(c.Do("GET", key))
		if err := proto.Unmarshal(in, &mytd); err != nil {
			log.Fatalln("Failed to parse address book:", err)
		}
		reload(&mytd)
	// 	for _, th := range val {
		 //	fmt.Println(th.(*rg.Thing).Location[0],".....",th.(*rg.Thing).Location[1],".....",th.(*rg.Thing).Name)
	 //	}
	}

/*
	t1 := time.Now()
	rt := &rg.Rtree{
		Dim:         2,
		MinChildren: 25,
		MaxChildren: 50,
		}
	  td:=rt.BulkLoad(things)



	proStu, err := proto.Marshal(td)
	if err != nil {
		fmt.Println("生成proStu字符串错误")
	}
	if err := ioutil.WriteFile("mydata.txt", proStu, 0644); err != nil {
		log.Fatalln("Failed to write address book:", err)
	}
	//jsonStu是[]byte类型，转化成string类型便于查看
	//fmt.Println(string(proStu))

	fmt.Println("size",i,rt.MYSize())

	if rt.MYSize() != i {
		fmt.Println("rt.Size() err")
	}

	pp:= rg.Point{120.231038,31.562171}
	bb := &rg.Thing{pp, "y"}

	// Get a slice of the objects in rt that intersect bb:

	results := rt.SearchIntersect(bb.Bounds(),rg.LimitFilter(10))
	fmt.Println("results....",len(results))

	for _, v := range results {
	//	fmt.Println(v.(*Thing).name) //prints a, b, c
	        fmt.Println(v) //prints a, b, c
	}
	fmt.Println(".........") //prints a, b, c

	// Get a slice of the k objects in rt closest to q:
	results,dists := rt.NearestNeighbors(5,pp)
	for _, v := range results {
		//	fmt.Println(v.(*Thing).name) //prints a, b, c
		fmt.Println(v) //prints a, b, c
	}
	for _, v := range dists {
		//	fmt.Println(v.(*Thing).name) //prints a, b, c
		fmt.Println(v) //prints a, b, c
	}
	t2 := time.Now()
	fmt.Println("首次加载+查询....",t2.Sub(t1).Seconds())

	fmt.Println("reload...");
	t3 := time.Now()

	in, err := ioutil.ReadFile("mydata.txt")
	if err != nil {
		log.Fatalln("Error reading file:", err)
	}
	var mytd pb.TreeData
	if err := proto.Unmarshal(in, &mytd); err != nil {
		log.Fatalln("Failed to parse address book:", err)
	}
	reload(&mytd)
	t4 := time.Now()
	fmt.Println("从文件中加载+查询....",t4.Sub(t3).Seconds())*/
}

func reload(td *pb.TreeData){



	rrt := &rg.Rtree{
		Dim:         int(td.Dim),
		MinChildren: int(td.MinChildren),
		MaxChildren: int(td.MaxChildren),
		 Height:int(td.Height),
		 Size:int(td.Size),
	}
	newentries := make([]rg.Entry,len(td.MyEntries))
	for i, en := range td.MyEntries {
		var an pb.Thing
	   ptypes.UnmarshalAny(en.Obj,&an)
		myeb:=rg.Entry{
			Bb:  &rg.Rect{P:en.Bb.P,Q:en.Bb.Q},
			Obj:&rg.Thing{Location: an.Location, Name: an.Name},
		}

		newentries[i]=myeb
	}
	rrt.Root=rrt.Omt(int(td.Height),int(td.NSlices),newentries ,int(td.NSubtree))
	fmt.Println("size",rrt.MYSize())



	pp:= rg.Point{120.231038,31.562171}
	bb := &rg.Thing{pp, "y"}

	// Get a slice of the objects in rt that intersect bb:

	results := rrt.SearchIntersect(bb.Bounds(),rg.LimitFilter(10))
	fmt.Println("results....",len(results))

	for _, v := range results {
		//	fmt.Println(v.(*Thing).name) //prints a, b, c
		fmt.Println(v) //prints a, b, c
	}
	fmt.Println(".........") //prints a, b, c

	// Get a slice of the k objects in rt closest to q:
	results,dists := rrt.NearestNeighbors(5,pp)
	for _, v := range results {
		//	fmt.Println(v.(*Thing).name) //prints a, b, c
		fmt.Println(v) //prints a, b, c
	}
	for _, v := range dists {
		//	fmt.Println(v.(*Thing).name) //prints a, b, c
		fmt.Println(v) //prints a, b, c
	}
}

func testRead() []byte {
	fp, err := os.OpenFile("data.json", os.O_RDONLY, 0755)
	defer fp.Close()
	if err != nil {
		log.Fatal(err)
	}
	data := make([]byte, 1000000)
	n, err := fp.Read(data)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(data[:n]))
	return data[:n]
}

func testWrite(data []byte) {
	fp, err := os.OpenFile("data.json", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()
	_, err = fp.Write(data)
	if err != nil {
		log.Fatal(err)
	}
}
func getRandomArbitrary(min float64, max float64)  float64 {
return rand.Float64()* (max - min) + min;
}


/**
 * 无锡经度随机
 */
func getNJLgt() float64{
return getRandomArbitrary(120.29	, 121.233);
}

/**
 * 无锡纬度随机
 */
func getNJLat() float64{
return getRandomArbitrary(31.49	, 32.35);
}