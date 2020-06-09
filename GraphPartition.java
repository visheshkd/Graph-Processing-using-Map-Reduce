import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent=new Vector<Long>();     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    public Vertex(){
      this.id=0;
      adjacent=new Vector<Long>();
      this.adjacent=null;
      this.centroid=-1;
      this.depth=0;
    }
    public Vertex(long id,Vector<Long> ad,long centroid,short depth){
        this.id=id;
        adjacent=new Vector<Long>();
        this.adjacent=ad;
        this.centroid=centroid;
        this.depth=depth;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeLong(centroid);
        dataOutput.writeShort(depth);
        dataOutput.writeLong(adjacent.size());
        for(int i=0;i<adjacent.size();i++)
            dataOutput.writeLong(adjacent.get(i));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        adjacent=new Vector<Long>();
        this.id=dataInput.readLong();
        this.centroid=dataInput.readLong();
        this.depth=dataInput.readShort();
        long vn = dataInput.readLong();
        for(int i=0;i<vn;i++) {
            this.adjacent.addElement(dataInput.readLong());
        }
    }
    public  String toString(){
        if(adjacent==null)
            return Long.toString(id)+","+ null +","+Long.toString(centroid)+","+Short.toString(depth);
        return Long.toString(id)+","+ adjacent.toString() +","+Long.toString(centroid)+","+Short.toString(depth);
    }

    /* ... */
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;
    static int vt=0;
    /* ... */
    public static class GphMapper1 extends Mapper<Object,Text,LongWritable,Vertex>{
        //Read the graph
        private LongWritable id=new LongWritable();
        public void map(LongWritable key,Vertex value,Context context) throws IOException, InterruptedException {
            Scanner s= new Scanner(value.toString()).useDelimiter(",");
            long centroid=-1;
            long id_new=s.nextLong();
            id.set(id_new);
            Vector<Long> ad= new Vector<Long>();
            while(s.hasNext()){
                ad.add(s.nextLong());
            }
            if(vt<10){
                centroid=id_new;
                centroids.add(id_new);
                vt++;
                context.write(id,new Vertex(id_new,ad,centroid,BFS_depth));
            }
            else
                context.write(id,new Vertex(id_new,ad,centroid,BFS_depth));

        }
    }
    public static class GphMapper2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex>{
        //BFS implementation
        public void map(LongWritable key,Vertex value,Context context) throws IOException, InterruptedException {
            Vector<Long> ad= new Vector<Long>();
            context.write(new LongWritable(value.id),value);//passing the graph topology
            if(value.centroid>0)
                for(long n:value.adjacent) //send the centroid to adjacent vertices
                    context.write(new LongWritable(n),new Vertex(n,ad,value.centroid,BFS_depth));

        }
    }
    public static class GphReducer2 extends Reducer<LongWritable,Vertex,LongWritable,Vertex>{
        public void reduce(LongWritable key,Iterable<Vertex> values,Context context) throws IOException, InterruptedException {
            short min_depth=1000;
            Vertex m=new Vertex(key.get(),new Vector<Long>(),-1,(short)0);
            for(Vertex v:values){
                if(!v.adjacent.isEmpty())
                    m.adjacent=v.adjacent;
                if(v.centroid>0 && v.depth<min_depth) {
                    min_depth = v.depth;
                    m.centroid=v.centroid;
                }

            }
            m.depth=min_depth;
            context.write(key,m);
        }
    }
    public static class GphMapper3 extends Mapper<LongWritable,Vertex,LongWritable,IntWritable>{
        // To calculate the cluster sizes
        public void map(LongWritable key,Vertex value,Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(value.centroid),new IntWritable(1));
        }

    }
    public static class GphReducer3 extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable>{
        public void reduce(LongWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int m=0;
            for(IntWritable v:values)
                m=m+v.get();
            context.write(key,new IntWritable(m));
        }
    }


    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");


        /* ... First Map-Reduce job to read the graph */
        job.setJarByClass(GraphPartition.class);
        job.setMapperClass(GphMapper1.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
            /* ... Second Map-Reduce job to do BFS */
            job.setJarByClass(GraphPartition.class);
            job.setMapperClass(GphMapper2.class);
            job.setReducerClass(GphReducer2.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job,new Path(args[1]+"/i"+i));
            FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i"+(i+1)));



            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the cluster sizes */
        job.setJarByClass(GraphPartition.class);
        job.setMapperClass(GphMapper3.class);
        job.setReducerClass(GphReducer3.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));


        job.waitForCompletion(true);
    }
}
