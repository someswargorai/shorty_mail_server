import mongoose from "mongoose";
import dotenv from "dotenv";
dotenv.config();

async function connect(){
    const connection = await mongoose.connect(
      `mongodb+srv://${process.env.MONGODB_USER}:${process.env.MONGODB_PASS}@cluster0.zbqlgv3.mongodb.net/url_shortner?appName=Cluster0`,
    );

    if(connection){
        console.log("DB connected")
    }
}

export default connect;
